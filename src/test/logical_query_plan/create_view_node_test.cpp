#include "gtest/gtest.h"

#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/view.hpp"

namespace opossum {

class CreateViewNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    _view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    _view = std::make_shared<View>(_view_node, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, {"a"}}});
    _create_view_node = CreateViewNode::make("some_view", _view);
  }

  std::shared_ptr<CreateViewNode> _create_view_node;
  std::shared_ptr<View> _view;
  std::shared_ptr<MockNode> _view_node;
};

TEST_F(CreateViewNodeTest, Description) {
  EXPECT_EQ(_create_view_node->description(),
            "[CreateView] Name: 'some_view' (\n"
            "[0] [MockNode 'Unnamed']\n"
            ")");
}

TEST_F(CreateViewNodeTest, Equals) {
  EXPECT_FALSE(lqp_find_subplan_mismatch(_create_view_node, _create_view_node));

  const auto same_create_view_node = CreateViewNode::make("some_view", _view);
  const auto different_create_view_node_a = CreateViewNode::make("some_view2", _view);

  const auto different_view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "b"}}));
  const auto different_view =
      std::make_shared<View>(different_view_node, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, {"b"}}});
  const auto different_create_view_node_b = CreateViewNode::make("some_view", different_view);

  EXPECT_TRUE(lqp_find_subplan_mismatch(different_create_view_node_a, _create_view_node).has_value());
  EXPECT_TRUE(lqp_find_subplan_mismatch(different_create_view_node_b, _create_view_node).has_value());
}

TEST_F(CreateViewNodeTest, Copy) {
  const auto same_view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
  const auto same_view =
      std::make_shared<View>(_view_node, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, {"a"}}});
  const auto same_create_view_node = CreateViewNode::make("some_view", _view);

  EXPECT_TRUE(!lqp_find_subplan_mismatch(same_create_view_node, _create_view_node->deep_copy()));
}

}  // namespace opossum
