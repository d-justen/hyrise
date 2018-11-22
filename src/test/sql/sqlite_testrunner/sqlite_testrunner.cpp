#include "sqlite_testrunner.hpp"

#include "constant_mappings.hpp"

namespace opossum {

TEST_P(SQLiteTestRunner, CompareToSQLite) {
  const auto& param = GetParam();

  const auto& sql = std::get<0>(param);
  const auto use_jit = std::get<1>(param);
  const auto encoding_type = std::get<2>(param);

  std::shared_ptr<LQPTranslator> lqp_translator;
  if (use_jit) {
    lqp_translator = std::make_shared<JitAwareLQPTranslator>();
  } else {
    lqp_translator = std::make_shared<LQPTranslator>();
  }

  SCOPED_TRACE("Query '" + sql + "'" + (use_jit ? " with JIT" : " without JIT") + " with encoding " + encoding_type_to_string.left.at(encoding_type));

  auto sql_pipeline = SQLPipelineBuilder{sql}.with_lqp_translator(lqp_translator).create_pipeline();

  const auto& result_table = sql_pipeline.get_result_table();

  for (const auto& plan : sql_pipeline.get_optimized_logical_plans()) {
    for (const auto& table_name : lqp_find_modified_tables(plan)) {
      // mark table cache entry as dirty, when table has been modified
      _table_cache_per_encoding.at(encoding_type).at(table_name).dirty = true;
    }
  }

  auto sqlite_result_table = _sqlite->execute_query(sql);

  // The problem is that we can only infer column types from sqlite if they have at least one row.
  ASSERT_TRUE(result_table && result_table->row_count() > 0 && sqlite_result_table &&
              sqlite_result_table->row_count() > 0)
        << "The SQLiteTestRunner cannot handle queries without results";

  auto order_sensitivity = OrderSensitivity::No;

  const auto& parse_result = sql_pipeline.get_parsed_sql_statements().back();
  if (parse_result->getStatements().front()->is(hsql::kStmtSelect)) {
    auto select_statement = dynamic_cast<const hsql::SelectStatement*>(parse_result->getStatements().back());
    if (select_statement->order != nullptr) {
      order_sensitivity = OrderSensitivity::Yes;
    }
  }

  ASSERT_TRUE(check_table_equal(result_table, sqlite_result_table, order_sensitivity, TypeCmpMode::Lenient,
                                FloatComparisonMode::RelativeDifference))
        << "Query failed: " << sql;

  // Delete newly created views in sqlite
  for (const auto& plan : sql_pipeline.get_optimized_logical_plans()) {
    if (const auto create_view = std::dynamic_pointer_cast<CreateViewNode>(plan)) {
      _sqlite->execute_query("DROP VIEW IF EXISTS " + create_view->view_name() + ";");
    }
  }
}

}  // namespace opossum
