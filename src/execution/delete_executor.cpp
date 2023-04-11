//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto transaction = exec_ctx_->GetTransaction();
  auto table_schema = table_info_->schema_;
  Tuple child_tuple;
  RID child_rid;
  bool res = child_executor_->Next(&child_tuple, &child_rid);

  if (res) {
    bool delete_res = table_info_->table_->MarkDelete(child_rid, transaction);
    if (!delete_res) {  // 抛出异常，需要进行相应处理
      throw Exception("delete failed");
    }
    Tuple key_tuple;
    for (auto info : index_info_) {  // 更新索引
      key_tuple = child_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      info->index_->DeleteEntry(key_tuple, child_rid, transaction);
    }
  }
  return res;
}

}  // namespace bustub
