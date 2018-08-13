#include "chunk_column_statistics.hpp"

#include <algorithm>
#include <iterator>
#include <type_traits>
#include <unordered_set>

#include "resolve_type.hpp"

#include "abstract_filter.hpp"
#include "histograms/equal_num_elements_histogram.hpp"
#include "min_max_filter.hpp"
#include "range_filter.hpp"
#include "storage/base_encoded_column.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/run_length_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
static std::shared_ptr<ChunkColumnStatistics> build_statistics_from_dictionary(const pmr_vector<T>& dictionary) {
  auto statistics = std::make_shared<ChunkColumnStatistics>();
  // only create statistics when the compressed dictionary is not empty
  if (!dictionary.empty()) {
    // no range filter for strings
    // clang-format off
    if constexpr(std::is_arithmetic_v<T>) {
      auto range_filter = RangeFilter<T>::build_filter(dictionary);
      statistics->add_filter(std::move(range_filter));
    } else {
      // we only need the min-max filter if we cannot have a range filter
      auto min_max_filter = std::make_unique<MinMaxFilter<T>>(dictionary.front(), dictionary.back());
      statistics->add_filter(std::move(min_max_filter));
    }
    // clang-format on
  }
  return statistics;
}

std::shared_ptr<ChunkColumnStatistics> ChunkColumnStatistics::build_statistics(
    DataType data_type, const std::shared_ptr<const BaseColumn>& column) {
  // std::shared_ptr<ChunkColumnStatistics> statistics;
  // resolve_data_and_column_type(*column, [&statistics](auto type, auto& typed_column) {
  //   using ColumnType = typename std::decay<decltype(typed_column)>::type;
  //   using DataTypeT = typename decltype(type)::type;
  //
  //   // clang-format off
  //   if constexpr(std::is_same_v<ColumnType, DictionaryColumn<DataTypeT>>) {
  //       // we can use the fact that dictionary columns have an accessor for the dictionary
  //       const auto& dictionary = *typed_column.dictionary();
  //       statistics = build_statistics_from_dictionary(dictionary);
  //   } else {
  //     // if we have a generic column we create the dictionary ourselves
  //     auto iterable = create_iterable_from_column<DataTypeT>(typed_column);
  //     std::unordered_set<DataTypeT> values;
  //     iterable.for_each([&](const auto& value) {
  //       // we are only interested in non-null values
  //       if (!value.is_null()) {
  //         values.insert(value.value());
  //       }
  //     });
  //     pmr_vector<DataTypeT> dictionary{values.cbegin(), values.cend()};
  //     std::sort(dictionary.begin(), dictionary.end());
  //     statistics = build_statistics_from_dictionary(dictionary);
  //   }
  //   // clang-format on
  // });
  // return statistics;

  auto statistics = std::make_shared<ChunkColumnStatistics>();

  resolve_data_and_column_type(*column, [&statistics, &column](auto type, auto& typed_column) {
    using ColumnType = typename std::decay<decltype(typed_column)>::type;
    using DataTypeT = typename decltype(type)::type;

    /**
     * Derive the number of buckets from the number of distinct elements iff we have a DictColumn.
     * This number is currently arbitrarily chosen.
     * TODO(tim): benchmark
     * Otherwise, take the default of 100 buckets.
     */
    size_t num_buckets = 100u;
    if constexpr (std::is_same_v<ColumnType, DictionaryColumn<DataTypeT>>) {
      const size_t proposed_buckets = typed_column.dictionary()->size() / 25;
      num_buckets = std::max(num_buckets, proposed_buckets);
    }

    if (std::is_same_v<DataTypeT, std::string>) {
      statistics->add_filter(EqualNumElementsHistogram<DataTypeT>::from_column(
          column, num_buckets,
          " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", 9));
    } else {
      statistics->add_filter(EqualNumElementsHistogram<DataTypeT>::from_column(column, num_buckets));
    }
  });

  return statistics;
}

void ChunkColumnStatistics::add_filter(std::shared_ptr<AbstractFilter> filter) { _filters.emplace_back(filter); }

bool ChunkColumnStatistics::can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                                      const std::optional<AllTypeVariant>& variant_value2) const {
  for (const auto& filter : _filters) {
    if (filter->can_prune(predicate_type, variant_value, variant_value2)) {
      return true;
    }
  }
  return false;
}
}  // namespace opossum
