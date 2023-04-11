//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the insert */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  void TupleSchemaTranformUseEvaluateJoin(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple,
                                          const Schema *right_schema, Tuple *dest_tuple, const Schema *dest_schema);
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  /** The child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  // 不能存储结果
  // std::vector<Tuple> right_tuples_;                       // 存储右半部所有元组
  // std::vector<Tuple>::const_iterator right_tuples_iter_;  // 记录当前访问位置
  bool left_need_next_;  // 是否需要执行左半部的next方法
  Tuple left_tuple_;     // 存储左半部当前的元组
  RID left_rid_;
  // bool right_need_next_;  // 是否需要执行右半部的next方法 不需要，永远需要执行右半部
  Tuple right_tuple_;  // 存储右半部当前的元组
  RID right_rid_;
};

}  // namespace bustub
