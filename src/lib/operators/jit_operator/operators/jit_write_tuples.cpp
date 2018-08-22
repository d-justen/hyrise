#include "jit_write_tuples.hpp"

#include "../jit_types.hpp"
#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/base_value_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

std::string JitWriteTuples::description() const {
  std::stringstream desc;
  desc << "[WriteTuple] ";
  for (const auto& output_column : _output_columns) {
    desc << output_column.column_name << " = x" << output_column.tuple_value.tuple_index() << ", ";
  }
  return desc.str();
}

std::shared_ptr<Table> JitWriteTuples::create_output_table(const ChunkOffset input_table_chunk_size) const {
  TableColumnDefinitions column_definitions;

  for (const auto& output_column : _output_columns) {
    // Add a column definition for each output column
    const auto data_type = output_column.tuple_value.data_type();
    const auto output_column_type = data_type == DataType::Bool ? DataTypeBool : data_type;
    const auto is_nullable = output_column.tuple_value.is_nullable();
    column_definitions.emplace_back(output_column.column_name, output_column_type, is_nullable);
  }

  return std::make_shared<Table>(column_definitions, TableType::Data, input_table_chunk_size);
}

void JitWriteTuples::before_query(const Table& in_table, Table& out_table, JitRuntimeContext& context) const {
  _create_output_chunk(context);
}

void JitWriteTuples::after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                                 JitRuntimeContext& context) const {
  if (!context.out_chunk.empty() && context.out_chunk[0]->size() > 0) {
    out_table.append_chunk(context.out_chunk);
    _create_output_chunk(context);
  }
}

void JitWriteTuples::add_output_column(const std::string& column_name, const JitTupleValue& value) {
  _output_columns.push_back({column_name, value});
}

std::vector<JitOutputColumn> JitWriteTuples::output_columns() const { return _output_columns; }

std::map<size_t, bool> JitWriteTuples::accessed_column_ids() const {
  std::map<size_t, bool> column_ids;
  for (const auto& column : _output_columns) {
    column_ids.insert_or_assign(column.tuple_value.tuple_index(), false);
  }
  return column_ids;
}

void JitWriteTuples::_consume(JitRuntimeContext& context) const {
  for (const auto& output : context.outputs) {
    output->write_value(context);
  }
}

void JitWriteTuples::_create_output_chunk(JitRuntimeContext& context) const {
  context.out_chunk.clear();
  context.outputs.clear();

  // Create new value columns and add them to the runtime context to make them accessible by the column writers
  for (const auto& output_column : _output_columns) {
    const auto data_type = output_column.tuple_value.data_type();
    const auto is_nullable = output_column.tuple_value.is_nullable();

    // Create the appropriate column writer for the output column
    if (data_type == DataType::Bool) {
      // Data type bool is a special jit data type and not an opossum data type
      // Therefore, bools needs to be handled separately as they are transformed to opossum int types
      auto column = std::make_shared<ValueColumn<Bool>>(output_column.tuple_value.is_nullable());
      context.out_chunk.push_back(column);
      if (is_nullable) {
        context.outputs.push_back(
            std::make_shared<JitColumnWriter<ValueColumn<Bool>, bool, true>>(column, output_column.tuple_value));
      } else {
        context.outputs.push_back(
            std::make_shared<JitColumnWriter<ValueColumn<Bool>, bool, false>>(column, output_column.tuple_value));
      }
    } else {
      // Opossum data types
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto column = std::make_shared<ValueColumn<ColumnDataType>>(output_column.tuple_value.is_nullable());
        context.out_chunk.push_back(column);

        if (is_nullable) {
          context.outputs.push_back(
              std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, true>>(
                  column, output_column.tuple_value));
        } else {
          context.outputs.push_back(
              std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, false>>(
                  column, output_column.tuple_value));
        }
      });
    }
  }
}

}  // namespace opossum
