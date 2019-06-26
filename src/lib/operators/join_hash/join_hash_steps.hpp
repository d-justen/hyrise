#pragma once

#include <boost/container/small_vector.hpp>
#include <boost/lexical_cast.hpp>

#include "bytell_hash_map.hpp"
#include "operators/multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "uninitialized_vector.hpp"

/*
  This file includes the functions that cover the main steps of our hash join implementation
  (e.g., build() and probe()). These free functions are put into this header file to separate
  them from the process flow of the join hash and to make the better testable.
*/
namespace opossum {

// For semi and anti joins, we only care whether a value exists or not, so there is no point in tracking the position
// in the input table of more than one occurrence of a value. However, if we have secondary predicates, we do need to
// track all occurrences of a value as that first position might be disqualified later.
enum class JoinHashBuildMode { AllPositions, SinglePosition };

using Hash = size_t;

/*
This is how elements of the input relations are saved after materialization.
The original value is used to detect hash collisions.
*/
template <typename T>
struct PartitionedElement {
  PartitionedElement() : row_id(NULL_ROW_ID), value(T()) {}
  PartitionedElement(RowID row, T val) : row_id(row), value(val) {}

  RowID row_id;
  T value;
};

// Initializing the partition vector takes some time. This is not necessary, because it will be overwritten anyway.
// The uninitialized_vector behaves like a regular std::vector, but the entries are initially invalid.
template <typename T>
using Partition = std::conditional_t<std::is_trivially_destructible_v<T>, uninitialized_vector<PartitionedElement<T>>,
                                     std::vector<PartitionedElement<T>>>;

// Stores the mapping from HashedType to positions. Conceptually, this is similar to an (unordered_)multimap, but it
// has some optimizations for the performance-critical probe() method. Instead of storing the matches directly in the
// hashmap (think map<HashedType, PosList>), we store an offset. This keeps the hashmap small and makes it easier to
// cache.
template <typename HashedType>
class PosHashTable {
  // In case we consider runtime to be more relevant, the flat hash map performs better (measured to be mostly on par
  // with bytell hash map and in some cases up to 5% faster) but is significantly larger than the bytell hash map.
  //
  // The hash table stores the relative offset of a SmallPosList (see below) in the vector of SmallPosLists. The range
  // of the offset does not limit the number of rows in the partition but the number of distinct values. If we end up
  // with a partition that has more values, the partitioning algorithm is at fault.
  using Offset = uint32_t;
  using HashTable = ska::bytell_hash_map<HashedType, Offset>;

  // The small_vector holds the first n values in local storage and only resorts to heap storage after that. 1 is chosen
  // as n because in many cases, we join on primary key attributes where by definition we have only one match on the
  // smaller side.
  using SmallPosList = boost::container::small_vector<RowID, 1>;

 public:
  explicit PosHashTable(const JoinHashBuildMode mode, const size_t max_size)
      : _hash_table(max_size), _pos_lists(max_size), _mode(mode) {}

  // For a value seen on the build side, add its row_id to the table
  template <typename InputType>
  void emplace(const InputType& value, RowID row_id) {
    const auto casted_value = static_cast<HashedType>(value);
    const auto it = _hash_table.find(casted_value);
    if (it != _hash_table.end()) {
      auto& pos_list = _pos_lists[it->second];
      if (_mode == JoinHashBuildMode::AllPositions) {
        pos_list.emplace_back(row_id);
      }
    } else {
      auto& pos_list = _pos_lists[_hash_table.size()];
      pos_list.push_back(row_id);
      _hash_table.emplace(casted_value, _hash_table.size());
      Assert(_hash_table.size() < std::numeric_limits<Offset>::max(), "Hash table too big for offset");
    }
  }

  // For a value seen on the probe side, return an iterator into the matching values
  template <typename InputType>
  const std::vector<SmallPosList>::const_iterator find(const InputType& value) const {
    const auto casted_value = static_cast<HashedType>(value);
    const auto it = _hash_table.find(casted_value);
    if (it == _hash_table.end()) return end();
    return _pos_lists.begin() + it->second;
  }

  const std::vector<SmallPosList>::const_iterator begin() const { return _pos_lists.begin(); }

  const std::vector<SmallPosList>::const_iterator end() const { return _pos_lists.end(); }

 private:
  HashTable _hash_table;
  std::vector<SmallPosList> _pos_lists;
  JoinHashBuildMode _mode;
};

/*
This struct contains radix-partitioned data in a contiguous buffer, as well as a list of offsets for each partition.
The offsets denote the accumulated sizes (we cannot use the last element's position because we could not recognize
empty first containers).

This struct is used in two phases:
  - the result of the materialization phase, at this time partitioned by chunks
    as we parallelize the materialization phase via chunks
  - the result of the radix clustering phase

As the radix clustering might be skipped (when radix_bits == 0), both the materialization as well as the radix
clustering methods yield RadixContainers.
*/
template <typename T>
struct RadixContainer {
  std::shared_ptr<Partition<T>> elements;
  std::vector<size_t> partition_offsets;

  // bit vector to store NULL flags
  std::shared_ptr<std::vector<bool>> null_value_bitvector;
};

inline std::vector<size_t> determine_chunk_offsets(const std::shared_ptr<const Table>& table) {
  const auto chunk_count = table->chunk_count();
  auto chunk_offsets = std::vector<size_t>(chunk_count);

  size_t offset = 0;
  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    chunk_offsets[chunk_id] = offset;
    offset += table->get_chunk(chunk_id)->size();
  }
  return chunk_offsets;
}

template <typename T, typename HashedType, bool retain_null_values>
RadixContainer<T> materialize_input(const std::shared_ptr<const Table>& in_table, ColumnID column_id,
                                    const std::vector<size_t>& chunk_offsets,
                                    std::vector<std::vector<size_t>>& histograms, const size_t radix_bits) {
  const std::hash<HashedType> hash_function;
  // list of all elements that will be partitioned
  auto elements = std::make_shared<Partition<T>>(in_table->row_count());

  [[maybe_unused]] auto null_value_bitvector = std::make_shared<std::vector<bool>>();
  if constexpr (retain_null_values) {
    null_value_bitvector->resize(in_table->row_count());
  }

  // fan-out
  const size_t num_partitions = 1ull << radix_bits;

  // currently, we just do one pass
  size_t pass = 0;
  size_t mask = static_cast<uint32_t>(pow(2, radix_bits * (pass + 1)) - 1);

  // create histograms per chunk
  histograms.resize(chunk_offsets.size());

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(in_table->chunk_count());

  for (ChunkID chunk_id{0}; chunk_id < in_table->chunk_count(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      // Get information from work queue
      auto output_offset = chunk_offsets[chunk_id];
      auto output_iterator = elements->begin() + output_offset;
      auto segment = in_table->get_chunk(chunk_id)->get_segment(column_id);

      [[maybe_unused]] auto null_value_bitvector_iterator = null_value_bitvector->begin();
      if constexpr (retain_null_values) {
        null_value_bitvector_iterator += output_offset;
      }

      // prepare histogram
      auto histogram = std::vector<size_t>(num_partitions);

      auto reference_chunk_offset = ChunkOffset{0};

      segment_with_iterators<T>(*segment, [&](auto it, const auto end) {
        using IterableType = typename decltype(it)::IterableType;

        while (it != end) {
          const auto& value = *it;
          ++it;

          if (!value.is_null() || retain_null_values) {
            // TODO(anyone): static_cast is almost always safe, since HashType is big enough. Only for double-vs-long
            // joins an information loss is possible when joining with longs that cannot be losslessly converted to
            // double
            const Hash hashed_value = hash_function(static_cast<HashedType>(value.value()));

            /*
            For ReferenceSegments we do not use the RowIDs from the referenced tables.
            Instead, we use the index in the ReferenceSegment itself. This way we can later correctly dereference
            values from different inputs (important for Multi Joins).
            */
            if constexpr (is_reference_segment_iterable_v<IterableType>) {
              *(output_iterator++) = PartitionedElement<T>{RowID{chunk_id, reference_chunk_offset}, value.value()};
            } else {
              *(output_iterator++) = PartitionedElement<T>{RowID{chunk_id, value.chunk_offset()}, value.value()};
            }

            // In case we care about NULL values, store the NULL flag
            if constexpr (retain_null_values) {
              if (value.is_null()) {
                *null_value_bitvector_iterator = true;
              }
            }

            const Hash radix = hashed_value & mask;
            ++histogram[radix];
            ++null_value_bitvector_iterator;
          }
          // reference_chunk_offset is only used for ReferenceSegments
          if constexpr (is_reference_segment_iterable_v<IterableType>) {
            ++reference_chunk_offset;
          }
        }
      });

      if constexpr (std::is_same_v<Partition<T>, uninitialized_vector<PartitionedElement<T>>>) {  // NOLINT
        // Because the vector is uninitialized, we need to manually fill up all slots that we did not use
        auto output_offset_end = chunk_id < chunk_offsets.size() - 1 ? chunk_offsets[chunk_id + 1] : elements->size();
        while (output_iterator != elements->begin() + output_offset_end) {
          *(output_iterator++) = PartitionedElement<T>{};
        }
      }

      histograms[chunk_id] = std::move(histogram);
    }));
    jobs.back()->schedule();
  }
  CurrentScheduler::wait_for_tasks(jobs);

  return RadixContainer<T>{elements, std::vector<size_t>{elements->size()}, null_value_bitvector};
}

/*
Build all the hash tables for the partitions of the build column. One job per partition
*/

template <typename BuildColumnType, typename HashedType>
std::vector<std::optional<PosHashTable<HashedType>>> build(const RadixContainer<BuildColumnType>& radix_container,
                                                           JoinHashBuildMode mode) {
  /*
  NUMA notes:
  The hash tables for each partition P should also reside on the same node as the two vectors buildP and probeP.
  */
  std::vector<std::optional<PosHashTable<HashedType>>> hash_tables;
  hash_tables.resize(radix_container.partition_offsets.size());

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(radix_container.partition_offsets.size());

  for (size_t current_partition_id = 0; current_partition_id < radix_container.partition_offsets.size();
       ++current_partition_id) {
    const auto build_partition_begin =
        current_partition_id == 0 ? 0 : radix_container.partition_offsets[current_partition_id - 1];
    const auto build_partition_end = radix_container.partition_offsets[current_partition_id];  // make end non-inclusive
    const auto build_partition_size = build_partition_end - build_partition_begin;

    // Skip empty partitions, so that we don't have too many empty hash tables
    if (build_partition_size == 0) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>(
        [&, build_partition_begin, build_partition_end, current_partition_id, build_partition_size]() {
          auto& build_partition = static_cast<Partition<BuildColumnType>&>(*radix_container.elements);

          auto hash_table = PosHashTable<HashedType>(mode, static_cast<size_t>(build_partition_size));

          for (size_t partition_offset = build_partition_begin; partition_offset < build_partition_end;
               ++partition_offset) {
            auto& element = build_partition[partition_offset];

            if (element.row_id == NULL_ROW_ID) {
              // Skip initialized PartitionedElements that might remain after materialization phase.
              continue;
            }

            hash_table.emplace(element.value, element.row_id);
          }

          hash_tables[current_partition_id] = std::move(hash_table);
        }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return hash_tables;
}

template <typename T, typename HashedType, bool retain_null_values>
RadixContainer<T> partition_radix_parallel(const RadixContainer<T>& radix_container,
                                           const std::vector<size_t>& chunk_offsets,
                                           std::vector<std::vector<size_t>>& histograms, const size_t radix_bits) {
  if constexpr (retain_null_values) {
    DebugAssert(radix_container.null_value_bitvector->size() == radix_container.elements->size(),
                "partition_radix_parallel() called with NULL consideration but radix container does not store any NULL "
                "value information");
  }

  const std::hash<HashedType> hash_function;

  // materialized items of radix container
  const auto& container_elements = *radix_container.elements;
  [[maybe_unused]] const auto& null_value_bitvector = *radix_container.null_value_bitvector;

  // fan-out
  const size_t num_partitions = 1ull << radix_bits;

  // currently, we just do one pass
  size_t pass = 0;
  size_t mask = static_cast<uint32_t>(pow(2, radix_bits * (pass + 1)) - 1);

  // allocate new (shared) output
  auto output = std::make_shared<Partition<T>>();
  output->resize(container_elements.size());

  [[maybe_unused]] auto output_nulls = std::make_shared<std::vector<bool>>();
  if constexpr (retain_null_values) {
    output_nulls->resize(null_value_bitvector.size());
  }

  RadixContainer<T> radix_output;
  radix_output.elements = output;
  radix_output.partition_offsets.resize(num_partitions);
  radix_output.null_value_bitvector = output_nulls;

  // use histograms to calculate partition offsets
  size_t offset = 0;
  std::vector<std::vector<size_t>> output_offsets_by_chunk(chunk_offsets.size(), std::vector<size_t>(num_partitions));
  for (size_t partition_id = 0; partition_id < num_partitions; ++partition_id) {
    for (ChunkID chunk_id{0}; chunk_id < chunk_offsets.size(); ++chunk_id) {
      output_offsets_by_chunk[chunk_id][partition_id] = offset;
      offset += histograms[chunk_id][partition_id];
    }
    radix_output.partition_offsets[partition_id] = offset;
  }

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(chunk_offsets.size());

  for (ChunkID chunk_id{0}; chunk_id < chunk_offsets.size(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      size_t input_offset = chunk_offsets[chunk_id];
      auto& output_offsets = output_offsets_by_chunk[chunk_id];

      size_t input_size = 0;
      if (chunk_id < chunk_offsets.size() - 1) {
        input_size = chunk_offsets[chunk_id + 1] - input_offset;
      } else {
        input_size = container_elements.size() - input_offset;
      }

      for (size_t chunk_offset = input_offset; chunk_offset < input_offset + input_size; ++chunk_offset) {
        auto& element = container_elements[chunk_offset];

        // In case of NULL-removing inner-joins, we ignore all NULL values.
        // Such values can be created in several ways: join input already has non-phyiscal NULL values (non-physical
        // means no RowID, e.g., created during an OUTER join), a physical value is NULL but is ignored for an inner
        // join (hence, we overwrite the RowID with NULL_ROW_ID), or it is simply a remainder of the pre-sized
        // RadixPartition which is initialized with default values (i.e., NULL_ROW_IDs).
        if (!retain_null_values && element.row_id == NULL_ROW_ID) {
          continue;
        }

        const size_t radix = hash_function(static_cast<HashedType>(element.value)) & mask;

        // In case NULL values have been materialized in materialize_input(),
        // we need to keep them during the radix clustering phase.
        if constexpr (retain_null_values) {
          (*output_nulls)[output_offsets[radix]] = null_value_bitvector[chunk_offset];
        }

        (*output)[output_offsets[radix]] = element;
        ++output_offsets[radix];
      }
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return radix_output;
}

/*
  In the probe phase we take all partitions from the probe partition, iterate over them and compare each join candidate
  with the values in the hash table. Since build and probe are hashed using the same hash function, we can reduce the
  number of hash tables that need to be looked into to just 1.
  */
template <typename ProbeColumnType, typename HashedType, bool keep_null_values>
void probe(const RadixContainer<ProbeColumnType>& probe_radix_container,
           const std::vector<std::optional<PosHashTable<HashedType>>>& hash_tables,
           std::vector<PosList>& pos_lists_build_side, std::vector<PosList>& pos_lists_probe_side, const JoinMode mode,
           const Table& build_table, const Table& probe_table,
           const std::vector<OperatorJoinPredicate>& secondary_join_predicates) {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(probe_radix_container.partition_offsets.size());

  /*
    NUMA notes:
    At this point both input relations are partitioned using radix partitioning.
    Probing will be done per partition for both sides.
    Therefore, inputs for one partition should be located on the same NUMA node,
    and the job that probes that partition should also be on that NUMA node.
  */
  for (size_t current_partition_id = 0; current_partition_id < probe_radix_container.partition_offsets.size();
       ++current_partition_id) {
    const auto partition_begin =
        current_partition_id == 0 ? 0 : probe_radix_container.partition_offsets[current_partition_id - 1];
    const auto partition_end = probe_radix_container.partition_offsets[current_partition_id];  // make end non-inclusive

    // Skip empty partitions to avoid empty output chunks
    if (partition_begin == partition_end) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_begin, partition_end, current_partition_id]() {
      // Get information from work queue
      auto& partition = static_cast<Partition<ProbeColumnType>&>(*probe_radix_container.elements);
      PosList pos_list_build_side_local;
      PosList pos_list_probe_local;

      if constexpr (keep_null_values) {
        DebugAssert(
            probe_radix_container.null_value_bitvector->size() == probe_radix_container.elements->size(),
            "Hash join probe called with NULL consideration but inputs do not store any NULL value information");
      }

      if (hash_tables[current_partition_id]) {
        const auto& hash_table = hash_tables.at(current_partition_id).value();

        // Accessors are not thread-safe, so we create one evaluator per job
        std::optional<MultiPredicateJoinEvaluator> multi_predicate_join_evaluator;
        if (!secondary_join_predicates.empty()) {
          multi_predicate_join_evaluator.emplace(build_table, probe_table, mode, secondary_join_predicates);
        }

        // simple heuristic to estimate result size: half of the partition's rows will match
        // a more conservative pre-allocation would be the size of the build cluster
        const size_t expected_output_size =
            static_cast<size_t>(std::max(10.0, std::ceil((partition_end - partition_begin) / 2)));
        pos_list_build_side_local.reserve(static_cast<size_t>(expected_output_size));
        pos_list_probe_local.reserve(static_cast<size_t>(expected_output_size));

        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& probe_column_element = partition[partition_offset];

          if (mode == JoinMode::Inner && probe_column_element.row_id == NULL_ROW_ID) {
            // From previous joins, we could potentially have NULL values that do not refer to
            // an actual probe_column_element but to the NULL_ROW_ID. Hence, we can only skip for inner joins.
            continue;
          }

          const auto& primary_predicate_matching_rows =
              hash_table.find(static_cast<HashedType>(probe_column_element.value));

          if (primary_predicate_matching_rows != hash_table.end()) {
            // Key exists, thus we have at least one hit for the primary predicate

            // Since we cannot store NULL values directly in off-the-shelf containers,
            // we need to the check the NULL bit vector here because a NULL value (represented
            // as a zero) yields the same rows as an actual zero value.
            // For inner joins, we skip NULL values and output them for outer joins.
            // Note, if the materialization/radix partitioning phase did not explicitly consider
            // NULL values, they will not be handed to the probe function.
            if constexpr (keep_null_values) {
              if ((*probe_radix_container.null_value_bitvector)[partition_offset]) {
                pos_list_build_side_local.emplace_back(NULL_ROW_ID);
                pos_list_probe_local.emplace_back(probe_column_element.row_id);
                // ignore found matches and continue with next probe item
                continue;
              }
            }

            // If NULL values are discarded, the matching probe_column_element pairs will be written to the result pos
            // lists.
            if (!multi_predicate_join_evaluator) {
              for (const auto& row_id : *primary_predicate_matching_rows) {
                pos_list_build_side_local.emplace_back(row_id);
                pos_list_probe_local.emplace_back(probe_column_element.row_id);
              }
            } else {
              auto match_found = false;
              for (const auto& row_id : *primary_predicate_matching_rows) {
                if (multi_predicate_join_evaluator->satisfies_all_predicates(row_id, probe_column_element.row_id)) {
                  pos_list_build_side_local.emplace_back(row_id);
                  pos_list_probe_local.emplace_back(probe_column_element.row_id);
                  match_found = true;
                }
              }

              // We have not found matching items for all predicates.
              if constexpr (keep_null_values) {
                if (!match_found) {
                  pos_list_build_side_local.emplace_back(NULL_ROW_ID);
                  pos_list_probe_local.emplace_back(probe_column_element.row_id);
                }
              }
            }

          } else {
            // We have not found matching items for the first predicate. Only continue for non-equi join modes.
            // We use constexpr to prune this conditional for the equi-join implementation.
            // Note, the outer relation (i.e., left relation for LEFT OUTER JOINs) is the probing
            // relation since the relations are swapped upfront.
            if constexpr (keep_null_values) {
              pos_list_build_side_local.emplace_back(NULL_ROW_ID);
              pos_list_probe_local.emplace_back(probe_column_element.row_id);
            }
          }
        }
      } else {
        // When there is no hash table, we might still need to handle the values of the probe side for LEFT
        // and RIGHT joins. We use constexpr to prune this conditional for the equi-join implementation.
        if constexpr (keep_null_values) {
          // We assume that the relations have been swapped previously, so that the outer relation is the probing
          // relation.
          // Since we did not find a hash table, we know that there is no match in the build column for this partition.
          // Hence we are going to write NULL values for each row.

          pos_list_build_side_local.reserve(partition_end - partition_begin);
          pos_list_probe_local.reserve(partition_end - partition_begin);

          for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
            auto& row = partition[partition_offset];
            pos_list_build_side_local.emplace_back(NULL_ROW_ID);
            pos_list_probe_local.emplace_back(row.row_id);
          }
        }
      }

      pos_lists_build_side[current_partition_id] = std::move(pos_list_build_side_local);
      pos_lists_probe_side[current_partition_id] = std::move(pos_list_probe_local);
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);
}

template <typename ProbeColumnType, typename HashedType, JoinMode mode>
void probe_semi_anti(const RadixContainer<ProbeColumnType>& radix_probe_column,
                     const std::vector<std::optional<PosHashTable<HashedType>>>& hash_tables,
                     std::vector<PosList>& pos_lists, const Table& build_table, const Table& probe_table,
                     const std::vector<OperatorJoinPredicate>& secondary_join_predicates) {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(radix_probe_column.partition_offsets.size());

  [[maybe_unused]] const auto* probe_column_null_values =
      radix_probe_column.null_value_bitvector ? radix_probe_column.null_value_bitvector.get() : nullptr;

  for (size_t current_partition_id = 0; current_partition_id < radix_probe_column.partition_offsets.size();
       ++current_partition_id) {
    const auto partition_begin =
        current_partition_id == 0 ? 0 : radix_probe_column.partition_offsets[current_partition_id - 1];
    const auto partition_end = radix_probe_column.partition_offsets[current_partition_id];  // make end non-inclusive

    // Skip empty partitions to avoid empty output chunks
    if (partition_begin == partition_end) {
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, partition_begin, partition_end, current_partition_id]() {
      // Get information from work queue
      auto& partition = static_cast<Partition<ProbeColumnType>&>(*radix_probe_column.elements);

      PosList pos_list_local;

      if (hash_tables[current_partition_id]) {
        // Valid hash table found, so there is at least one match in this partition

        // Accessors are not thread-safe, so we create one evaluator per job
        MultiPredicateJoinEvaluator multi_predicate_join_evaluator(build_table, probe_table, mode,
                                                                   secondary_join_predicates);

        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& probe_column_element = partition[partition_offset];

          if constexpr (mode == JoinMode::Semi) {
            // NULLs on the probe side are never emitted
            if (probe_column_element.row_id.chunk_offset == INVALID_CHUNK_OFFSET) {
              continue;
            }
          } else if constexpr (mode == JoinMode::AntiNullAsFalse) {  // NOLINT - doesn't like else if constexpr
            // NULL values on the probe side always lead to the tuple being emitted for AntiNullAsFalse, irrespective
            // of secondary predicates (`NULL("as false") AND <anything>` is always false)
            if ((*probe_column_null_values)[partition_offset]) {
              pos_list_local.emplace_back(probe_column_element.row_id);
              continue;
            }
          } else if constexpr (mode == JoinMode::AntiNullAsTrue) {  // NOLINT - doesn't like else if constexpr
            if ((*probe_column_null_values)[partition_offset]) {
              // Primary predicate is TRUE, as long as we do not support secondary predicates with AntiNullAsTrue.
              // This means that the probe value never gets emitted
              continue;
            }
          }

          auto any_build_column_value_matches = false;
          const auto& hash_table = hash_tables[current_partition_id].value();
          const auto& primary_predicate_matching_rows =
              hash_table.find(static_cast<HashedType>(probe_column_element.value));

          if (primary_predicate_matching_rows != hash_table.end()) {
            for (const auto& row_id : *primary_predicate_matching_rows) {
              if (multi_predicate_join_evaluator.satisfies_all_predicates(row_id, probe_column_element.row_id)) {
                any_build_column_value_matches = true;
                break;
              }
            }
          }

          if ((mode == JoinMode::Semi && any_build_column_value_matches) ||
              ((mode == JoinMode::AntiNullAsTrue || mode == JoinMode::AntiNullAsFalse) &&
               !any_build_column_value_matches)) {
            pos_list_local.emplace_back(probe_column_element.row_id);
          }
        }
      } else if constexpr (mode == JoinMode::AntiNullAsFalse) {  // NOLINT - doesn't like else if constexpr
        // no hash table on other side, but we are in AntiNullAsFalse mode which means all tuples from the probing side
        // get emitted.
        pos_list_local.reserve(partition_end - partition_begin);
        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& probe_column_element = partition[partition_offset];
          pos_list_local.emplace_back(probe_column_element.row_id);
        }
      } else if constexpr (mode == JoinMode::AntiNullAsTrue) {  // NOLINT - doesn't like else if constexpr
        // no hash table on other side, but we are in AntiNullAsTrue mode which means all tuples from the probing side
        // get emitted. That is, except NULL values, which only get emitted if the build table is empty.
        pos_list_local.reserve(partition_end - partition_begin);
        for (size_t partition_offset = partition_begin; partition_offset < partition_end; ++partition_offset) {
          auto& probe_column_element = partition[partition_offset];
          // A NULL on the probe side never gets emitted, except when the build table is empty.
          // This is because `NULL NOT IN <empty list>` is actually true
          if ((*probe_column_null_values)[partition_offset] && build_table.row_count() > 0) {
            continue;
          }
          pos_list_local.emplace_back(probe_column_element.row_id);
        }
      }

      pos_lists[current_partition_id] = std::move(pos_list_local);
    }));
    jobs.back()->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);
}

using PosLists = std::vector<std::shared_ptr<const PosList>>;
using PosListsByChunk = std::vector<std::shared_ptr<PosLists>>;

/**
 * Returns a vector where each entry with index i references a PosLists object. The PosLists object
 * contains the position list of every segment/chunk in column i.
 * @param input_table
 */
// See usage in _on_execute() for doc.
inline PosListsByChunk setup_pos_lists_by_chunk(const std::shared_ptr<const Table>& input_table) {
  DebugAssert(input_table->type() == TableType::References, "Function only works for reference tables");

  std::map<PosLists, std::shared_ptr<PosLists>> shared_pos_lists_by_pos_lists;

  PosListsByChunk pos_lists_by_segment(input_table->column_count());
  auto pos_lists_by_segment_it = pos_lists_by_segment.begin();

  const auto& input_chunks = input_table->chunks();

  // For every column, for every chunk
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    // Get all the input pos lists so that we only have to pointer cast the segments once
    auto pos_list_ptrs = std::make_shared<PosLists>(input_table->chunk_count());
    auto pos_lists_iter = pos_list_ptrs->begin();

    // Iterate over every chunk and add the chunks segment with column_id to pos_list_ptrs
    for (ChunkID chunk_id{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      const auto& ref_segment_uncasted = input_chunks[chunk_id]->segments()[column_id];
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(ref_segment_uncasted);
      *pos_lists_iter = ref_segment->pos_list();
      ++pos_lists_iter;
    }

    // pos_list_ptrs contains all position lists of the reference segments for the column_id.
    auto iter = shared_pos_lists_by_pos_lists.emplace(*pos_list_ptrs, pos_list_ptrs).first;

    *pos_lists_by_segment_it = iter->second;
    ++pos_lists_by_segment_it;
  }

  return pos_lists_by_segment;
}

/**
 *
 * @param output_segments [in/out] Vector to which the newly created reference segments will be written.
 * @param input_table Table which all the position lists reference
 * @param input_pos_list_ptrs_sptrs_by_segments Contains all position lists to all columns of input table
 * @param pos_list contains the positions of rows to use from the input table
 */
inline void write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                  const PosListsByChunk& input_pos_list_ptrs_sptrs_by_segments,
                                  std::shared_ptr<PosList> pos_list) {
  std::map<std::shared_ptr<PosLists>, std::shared_ptr<PosList>> output_pos_list_cache;

  // We might use this later, but want to have it outside of the for loop
  std::shared_ptr<Table> dummy_table;

  // Add segments from input table to output chunk
  // for every column for every row in pos_list: get corresponding PosList of input_pos_list_ptrs_sptrs_by_segments
  // and add it to new_pos_list which is added to output_segments
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        const auto& input_table_pos_lists = input_pos_list_ptrs_sptrs_by_segments[column_id];

        auto iter = output_pos_list_cache.find(input_table_pos_lists);
        if (iter == output_pos_list_cache.end()) {
          // Get the row ids that are referenced
          auto new_pos_list = std::make_shared<PosList>(pos_list->size());
          auto new_pos_list_iter = new_pos_list->begin();
          for (const auto& row : *pos_list) {
            if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
              *new_pos_list_iter = row;
            } else {
              const auto& referenced_pos_list = *(*input_table_pos_lists)[row.chunk_id];
              *new_pos_list_iter = referenced_pos_list[row.chunk_offset];
            }
            ++new_pos_list_iter;
          }

          iter = output_pos_list_cache.emplace(input_table_pos_lists, new_pos_list).first;
        }

        auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
            input_table->get_chunk(ChunkID{0})->get_segment(column_id));
        output_segments.push_back(std::make_shared<ReferenceSegment>(
            reference_segment->referenced_table(), reference_segment->referenced_column_id(), iter->second));
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        if (!dummy_table) dummy_table = Table::create_dummy_table(input_table->column_definitions());
        output_segments.push_back(std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list));
      }
    } else {
      output_segments.push_back(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }
  }
}

}  // namespace opossum
