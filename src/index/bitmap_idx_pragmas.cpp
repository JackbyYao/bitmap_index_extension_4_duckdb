
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include "index/bitmap_idx.hpp"
#include "index/bitmap_idx_module.hpp"

namespace duckdb {

// BIND
static unique_ptr<FunctionData> BitmapindexInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	//TODO: maybe we can add more info here
	names.emplace_back("catalog_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("index_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

// INIT GLOBAL
struct BitmapIndexInfoState final : public GlobalTableFunctionState {
	idx_t offset = 0;
	vector<reference<IndexCatalogEntry>> entries;
};

static unique_ptr<GlobalTableFunctionState> BitmapIndexInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<BitmapIndexInfoState>();

	// scan all the schemas for indexes and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
			auto &index_entry = entry.Cast<IndexCatalogEntry>();
			if (index_entry.index_type == BitmapIndex::TYPE_NAME) {
				result->entries.push_back(index_entry);
			}
		});
	};
	return std::move(result);
}

// EXECUTE
static void BitmapIndexInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<BitmapIndexInfoState>();
	if (data.offset >= data.entries.size()) {
		return;
	}

	idx_t row = 0;
	while (data.offset < data.entries.size() && row < STANDARD_VECTOR_SIZE) {
		auto &index_entry = data.entries[data.offset++].get();
		auto &table_entry = index_entry.schema.catalog.GetEntry<TableCatalogEntry>(context, index_entry.GetSchemaName(),
		                                                                           index_entry.GetTableName());
		auto &storage = table_entry.GetStorage();
		BitmapIndex *bitmap_index_ptr = nullptr;

		auto &table_info = *storage.GetDataTableInfo();
		table_info.BindIndexes(context, BitmapIndex::TYPE_NAME);
		table_info.GetIndexes().Scan([&](Index &index) {
			if (!index.IsBound() || BitmapIndex::TYPE_NAME != index.GetIndexType()) {
				return false;
			}
			auto &bitmap_index = index.Cast<BitmapIndex>();
			if (bitmap_index.name == index_entry.name) {
				bitmap_index_ptr = &bitmap_index;
				return true;
			}
			return false;
		});

		if (!bitmap_index_ptr) {
			throw BinderException("Index %s not found", index_entry.name);
		}

		idx_t col = 0;

		output.data[col++].SetValue(row, Value(index_entry.catalog.GetName()));
		output.data[col++].SetValue(row, Value(index_entry.schema.name));
		output.data[col++].SetValue(row, Value(index_entry.name));
		output.data[col++].SetValue(row, Value(table_entry.name));

		row++;
	}
	output.SetCardinality(row);

}

static optional_ptr<BitmapIndex> TryGetIndex(ClientContext &context, const string &index_name) {
    auto qname = QualifiedName::Parse(index_name);

    // look up the index name in the catalog
    Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
    auto &index_entry = Catalog::GetEntry(context, CatalogType::INDEX_ENTRY, qname.catalog, qname.schema, qname.name)
                            .Cast<IndexCatalogEntry>();
    auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qname.catalog, 
                                          index_entry.GetSchemaName(), index_entry.GetTableName())
                            .Cast<TableCatalogEntry>();

    auto &storage = table_entry.GetStorage();
    BitmapIndex *bitmap_index = nullptr;

    auto &table_info = *storage.GetDataTableInfo();
    table_info.BindIndexes(context, BitmapIndex::TYPE_NAME);
    table_info.GetIndexes().Scan([&](Index &index) {
        if (!index.IsBound() || BitmapIndex::TYPE_NAME != index.GetIndexType()) {
            return false;
        }
        auto &bitmap_idx = index.Cast<BitmapIndex>();
        if (index_entry.name == index_name) {
            bitmap_index = &bitmap_idx;
            return true;
        }
        return false;
    });
    return bitmap_index;
}

//-------------------------------------------------------------------------
// Bitmap Index Dump
//-------------------------------------------------------------------------
// BIND
struct BitmapIndexDumpBindData final : public TableFunctionData {
	string index_name;
};

static unique_ptr<FunctionData> BitmapIndexDumpBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<BitmapIndexDumpBindData>();

	result->index_name = input.inputs[0].GetValue<string>();

	// Return types
	names.emplace_back("distinct_value");
    return_types.emplace_back(LogicalType::VARCHAR);  // The indexed value
    names.emplace_back("bitmap_mem_size");
    return_types.emplace_back(LogicalType::UBIGINT);  // Storage size
    names.emplace_back("bitmap_bit_size");
    return_types.emplace_back(LogicalType::UBIGINT);  // Index size 
	names.emplace_back("compression_ratio");
    return_types.emplace_back(LogicalType::DOUBLE);   // Compression effectiveness

	return std::move(result);
}

// INIT

struct BitmapIndexDumpState final : public GlobalTableFunctionState {
	const BitmapIndex &index;
	vector<string> distinct_values;
	idx_t current_value_index = 0;

public:
	explicit BitmapIndexDumpState(BitmapIndex &index) : index(index) {
		distinct_values = index.GetDistinctValues();
	}
};

static unique_ptr<GlobalTableFunctionState> BitmapIndexDumpInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<BitmapIndexDumpBindData>();
	auto bitmap_index = TryGetIndex(context, bind_data.index_name);
	if(!bitmap_index) {
        throw BinderException("Bitmap index %s not found", bind_data.index_name);
    }
	return make_uniq<BitmapIndexDumpState>(*bitmap_index);
}

// EXECUTE
static void BitmapIndexDumpExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	    auto &state = data_p.global_state->Cast<BitmapIndexDumpState>();
    
    if (state.current_value_index >= state.distinct_values.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t output_idx = 0;
    
    // Get references to the output vectors
    auto &distinct_value_vec = output.data[0];
    auto &in_memory_size_vec = output.data[1];
    auto &index_size_vec = output.data[2];
    auto &compression_ratio_vec = output.data[3];
    // ... other vectors

    while (state.current_value_index < state.distinct_values.size() && output_idx < STANDARD_VECTOR_SIZE) {
        const auto &value = state.distinct_values[state.current_value_index];
        
        // Get the statistics
        auto mem_size = state.index.GetInMemorySize();
        auto idx_size = state.index.GetIndexSize();
        auto comp_ratio = state.index.GetCompressionRatio();

        // Set values using Vector::SetValue method
        distinct_value_vec.SetValue(output_idx, Value(value));
        in_memory_size_vec.SetValue(output_idx, Value::UBIGINT(mem_size));
        index_size_vec.SetValue(output_idx, Value::UBIGINT(idx_size));
        compression_ratio_vec.SetValue(output_idx, Value::UBIGINT(comp_ratio));
        
        output_idx++;
        state.current_value_index++;
    }
    
    output.SetCardinality(output_idx);
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void BitmapIndexModule::RegisterIndexPragmas(ExtensionLoader &loader) {

	TableFunction info_function("pragma_bitmap_index_info", {}, BitmapIndexInfoExecute, BitmapindexInfoBind,
	                            BitmapIndexInfoInit);

	loader.RegisterFunction(info_function);

	TableFunction dump_function("bitmap_index_dump", {LogicalType::VARCHAR}, BitmapIndexDumpExecute, BitmapIndexDumpBind,
	                            BitmapIndexDumpInit);

	loader.RegisterFunction(dump_function);
}

} // namespace duckdb
