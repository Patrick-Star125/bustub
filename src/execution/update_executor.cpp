//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto transaction = exec_ctx_->GetTransaction();
  auto table_schema = table_info_->schema_;
  Tuple child_tuple;  // 原来的元组
  RID child_rid;
  Tuple update_tuple;  // 更新后的元组
  bool res = child_executor_->Next(&child_tuple, &child_rid);

  if (res) {
    update_tuple = GenerateUpdatedTuple(child_tuple);
    table_info_->table_->UpdateTuple(update_tuple, child_rid, transaction);  // 传入old rid

    Tuple old_key_tuple;
    Tuple new_key_tuple;
    for (auto info : index_info_) {  // 更新索引，RID为子执行器输出RID
      old_key_tuple = child_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      new_key_tuple = update_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      info->index_->DeleteEntry(old_key_tuple, child_rid, transaction);
      info->index_->InsertEntry(new_key_tuple, child_rid, transaction);
    }
  }
  return res;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
