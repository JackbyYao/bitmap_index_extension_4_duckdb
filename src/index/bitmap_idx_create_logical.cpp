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
    throw NotImplementedException("LogicalCreateBitmapIndex::LogicalCreateBitmapIndex() not implemented");
}

void LogicalCreateBitmapIndex::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
    throw NotImplementedException("LogicalCreateBitmapIndex::ResolveTypes() not implemented");
}

void LogicalCreateBitmapIndex::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	bindings = LogicalOperator::GenerateColumnBindings(0, table.GetColumns().LogicalColumnCount());
    throw NotImplementedException("LogicalCreateBitmapIndex::ResolveColumnBindings() not implemented");
}

static PhysicalOperator &CreateNullFilter(PhysicalPlanGenerator &generator, const LogicalOperator &op,
                                          const vector<LogicalType> &types, ClientContext &context) {
	throw NotImplementedException("CreateNullFilter() not implemented");
}


PhysicalOperator &BitmapIndex::CreatePlan(PlanIndexInput &input) {

	throw NotImplementedException("BitmapIndex::CreatePlan() not implemented");

}

// TODO: Remove this
PhysicalOperator &LogicalCreateBitmapIndex::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	throw NotImplementedException("BitmapIndex::CreatePlan() not implemented");
}

void LogicalCreateBitmapIndex::Serialize(Serializer &writer) const {
	LogicalExtensionOperator::Serialize(writer);
	writer.WritePropertyWithDefault(300, "operator_type", string(OPERATOR_TYPE_NAME));
	writer.WritePropertyWithDefault<unique_ptr<CreateIndexInfo>>(400, "info", info);
	writer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions", unbound_expressions);
    throw NotImplementedException("LogicalCreateBitmapIndex::Serialize() not implemented");
}

unique_ptr<LogicalExtensionOperator> LogicalCreateBitmapIndex::Deserialize(Deserializer &reader) {
	auto create_info = reader.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(400, "info");
	auto unbound_expressions =
	    reader.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions");

	auto info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(create_info));

	// We also need to rebind the table
	auto &context = reader.Get<ClientContext &>();
	const auto &catalog = info->catalog;
	const auto &schema = info->schema;
	const auto &table_name = info->table;
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);
    throw NotImplementedException("LogicalCreateBitmapIndex::Deserialize() not implemented");
	// Return the new operator
	return make_uniq_base<LogicalExtensionOperator, LogicalCreateBitmapIndex>(
	    std::move(info), std::move(unbound_expressions), table_entry);
}

} // namespace duckdb
