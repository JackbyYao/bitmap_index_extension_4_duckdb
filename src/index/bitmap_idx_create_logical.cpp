#include "index/bitmap_idx_create_logical.hpp"
#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_create_physical.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

namespace duckdb {

LogicalCreateBitmapIndex::LogicalCreateBitmapIndex(unique_ptr<CreateIndexInfo> info_p,
                                                 vector<unique_ptr<Expression>> expressions_p,
                                                 TableCatalogEntry &table_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), table(table_p) {
	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);
	// DUMMY: 构造函数完成
}

void LogicalCreateBitmapIndex::ResolveTypes() {
	// DUMMY: 返回BIGINT类型（表示创建的索引数量，通常为1）
	types.emplace_back(LogicalType::BIGINT);
}

void LogicalCreateBitmapIndex::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	// DUMMY: 生成表的所有列绑定
	bindings = LogicalOperator::GenerateColumnBindings(0, table.GetColumns().LogicalColumnCount());
	// Visit the operator's expressions
	LogicalOperatorVisitor::EnumerateExpressions(*this,
	                                             [&](unique_ptr<Expression> *child) { res.VisitExpression(child); });
}

/*
// Note: we allow NULL value so this function is not needed
static PhysicalOperator &CreateNullFilter(PhysicalPlanGenerator &generator, const LogicalOperator &op,
                                          const vector<LogicalType> &types, ClientContext &context) {
	throw NotImplementedException("CreateNullFilter() not implemented");
}*/


PhysicalOperator &BitmapIndex::CreatePlan(PlanIndexInput &input) {
	// DUMMY: 创建物理执行计划
	// 参考ART的plan_art.cpp实现

	auto &op = input.op;
	auto &planner = input.planner;
	/*
	// generate a physical plan for the parallel index creation which consists of the following operators
	// table scan - projection (for expression execution) - filter - order - create index
	auto &table_scan = input.table_scan;
    auto &context = input.context;
	D_ASSERT(op.children.size() == 1);

	if (op.unbound_expressions.size() != 1) {
			throw BinderException("Bitmap index must be created on one column or expression.");
		}
	auto &expr = op.unbound_expressions[0];

    if (expr->return_type.id() == LogicalTypeId::INVALID) {
        throw BinderException("Bitmap index expression has invalid type.");
	}
	// projection to execute expressions on the key columns
	vector<LogicalType> new_column_types;
    vector<unique_ptr<Expression>> select_list;
	// TODO: add filter operator/project/ORDERBY?
    */

	// DUMMY简化版本：直接创建PhysicalCreateBitmapIndex，不添加PROJECTION/FILTER/ORDER
	// 实际实现应该像ART一样添加这些算子来准备数据
	auto &create_idx = planner.Make<PhysicalCreateBitmapIndex>(
		op,
		op.table,
		op.info->column_ids,
		std::move(op.info),
		std::move(op.unbound_expressions),
		op.estimated_cardinality
	);

	// 将table_scan作为子节点
	create_idx.children.push_back(input.table_scan);

	return create_idx;
}

// TODO: Remove this - 这个函数可能已废弃
PhysicalOperator &LogicalCreateBitmapIndex::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	// DUMMY: 这个函数可能不会被调用，因为我们使用IndexType::create_plan
	throw NotImplementedException("LogicalCreateBitmapIndex::CreatePlan() should not be called");
}

void LogicalCreateBitmapIndex::Serialize(Serializer &writer) const {
	// DUMMY: 序列化逻辑算子（用于计划缓存或分布式执行）
	LogicalExtensionOperator::Serialize(writer);
	writer.WritePropertyWithDefault(300, "operator_type", string(OPERATOR_TYPE_NAME));
	writer.WritePropertyWithDefault<unique_ptr<CreateIndexInfo>>(400, "info", info);
	writer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions", unbound_expressions);
}

unique_ptr<LogicalExtensionOperator> LogicalCreateBitmapIndex::Deserialize(Deserializer &reader) {
	// DUMMY: 反序列化逻辑算子
	auto create_info = reader.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(400, "info");
	auto unbound_expressions =
	    reader.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions");

	auto info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(create_info));

	// 重新绑定表
	auto &context = reader.Get<ClientContext &>();
	const auto &catalog = info->catalog;
	const auto &schema = info->schema;
	const auto &table_name = info->table;
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);

	// 返回新的算子
	return make_uniq_base<LogicalExtensionOperator, LogicalCreateBitmapIndex>(
	    std::move(info), std::move(unbound_expressions), table_entry);
}

} // namespace duckdb
