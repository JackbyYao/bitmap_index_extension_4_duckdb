#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

class LogicalBitmapIndexJoin final : public LogicalExtensionOperator {
public:
	static constexpr auto OPERATOR_TYPE_NAME = "logical_bitmap_index_join";

	// Join metadata
	JoinType join_type;
	vector<JoinCondition> conditions;

	// Bitmap index reference (stored as strings for serialization safety)
	string bitmap_index_schema;
	string bitmap_index_table_name;
	string bitmap_index_name;
	
	// Build/probe side info
	// bitmap_index_table_index: 0 = left child, 1 = right child
	idx_t bitmap_index_table_index;
	bool build_on_left;

	// Projection maps (from LogicalJoin)
	vector<idx_t> left_projection_map;
	vector<idx_t> right_projection_map;

	// Join statistics (from LogicalJoin)
	vector<unique_ptr<BaseStatistics>> join_stats;

public:
	LogicalBitmapIndexJoin(JoinType join_type, vector<JoinCondition> conditions,
	                       string bitmap_index_schema, string bitmap_index_table_name, string bitmap_index_name,
	                       idx_t bitmap_index_table_index, bool build_on_left,
	                       vector<idx_t> left_projection_map, vector<idx_t> right_projection_map,
	                       vector<unique_ptr<BaseStatistics>> join_stats);

	void ResolveTypes() override;
	vector<ColumnBinding> GetColumnBindings() override;
	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override;

	// Create physical plan
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

public:
	void Serialize(Serializer &writer) const override;
	static unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &reader);

public:
	string GetName() const override {
		return "BITMAP_INDEX_JOIN";
	}

	string GetExtensionName() const override {
		return "bitmap_idx";
	}
};

} // namespace duckdb

