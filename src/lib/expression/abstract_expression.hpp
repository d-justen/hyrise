#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

enum class ExpressionType {
  Aggregate, Arithmetic, Case, Column, Exists, Extract, Function, Negate, List, Logical, Parameter, Predicate, Select, Value
};

/**
 * AbstractExpression is the self-contained data structure describing Expressions in Hyrise.
 *
 * Expressions in Hyrise are everything down from Literals and Columns, over Arithmetics (a + b, ...),
 * Logicals (a AND b), up to Lists (`('a', 'b')`) and Subselects. Check out the classes derived from AbstractExpression
 * for all available types.
 *
 * Expressions are evaluated (typically for all rows of a Chunk) using the ExpressionEvaluator.
 */
class AbstractExpression : public std::enable_shared_from_this<AbstractExpression> {
 public:
  explicit AbstractExpression(const ExpressionType type, const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  virtual ~AbstractExpression() = default;

  /**
   * Recursively check for Expression equality.
   * @pre Both expressions need to reference the same LQP
   */
  bool deep_equals(const AbstractExpression& other) const;

  virtual bool requires_calculation() const;

  virtual std::shared_ptr<AbstractExpression> deep_copy() const = 0;

  /**
   * @return a human readable string representing the Expression that can be used as a column name
   */
  virtual std::string as_column_name() const = 0;

  /**
   * @return the DataType of the result of the expression
   */
  virtual DataType data_type() const = 0;

  /**
   * @return whether the result of the Expression MAY contain a NULL
   */
  virtual bool is_nullable() const;

  size_t hash() const;

  const ExpressionType type;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;

 protected:
  /**
   * Override to check for equality without checking the arguments
   */
  virtual bool _shallow_equals(const AbstractExpression& expression) const;

  /**
   * Override to hash data fields in derived types
   */
  virtual size_t _on_hash() const;

  /**
   * Used internally in _enclose_argument_as_column_name() to put parentheses around expression arguments if they have a lower
   * precedence than the expression itself.
   * Lower precedence indicates tighter binding, compare https://en.cppreference.com/w/cpp/language/operator_precedence
   *
   * @return  0 by default
   */
  virtual uint32_t _precedence() const;

  /**
   * @return    argument.as_column_name(), enclosed by parentheses if the argument precedence is lower than
   *            this->_precedence()
   */
  std::string _enclose_argument_as_column_name(const AbstractExpression& argument) const;
};

// Wrapper around expression->hash(), to enable hash based containers containing std::shared_ptr<AbstractExpression>
struct ExpressionSharedPtrHash final {
  size_t operator()(const std::shared_ptr<AbstractExpression>& expression) const {
    return expression->hash();
  }
};

// Wrapper around expression->deep_equals(), to enable hash based containers containing
// std::shared_ptr<AbstractExpression>
struct ExpressionSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<AbstractExpression>& expression_a,
                    const std::shared_ptr<AbstractExpression>& expression_b) const {
    return expression_a->deep_equals(*expression_b);
  }
};

template<typename Value> using ExpressionUnorderedMap = std::unordered_map<std::shared_ptr<AbstractExpression>, Value, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;
using ExpressionUnorderedSet = std::unordered_set<std::shared_ptr<AbstractExpression>, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

}  // namespace opossum