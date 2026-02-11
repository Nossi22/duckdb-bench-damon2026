#define DUCKDB_EXTENSION_MAIN

#include "view_rewriter_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <sstream>

namespace duckdb {

// ──────────────────────────────────────────────────────────────────────
// RewriteRule: describes how to match a FILTER(SCAN(table)) pattern
// and what pre-filtered view name to substitute.
// ──────────────────────────────────────────────────────────────────────
struct RewriteRule {
	//! The source table name to match (e.g., "lineitem")
	string source_table;
	//! A human-readable description of the filter predicate to match.
	//! The actual matching is done via a fingerprint string.
	string filter_description;
	//! The fingerprint of the filter expression tree (serialized form)
	//! Used for exact structural matching.
	string filter_fingerprint;
	//! The name of the pre-filtered view/table to replace the scan with
	string replacement_view;
};

// ──────────────────────────────────────────────────────────────────────
// Extension info: holds registered rewrite rules and statistics
// ──────────────────────────────────────────────────────────────────────
struct ViewRewriterInfo : public OptimizerExtensionInfo {
	vector<RewriteRule> rules;
	idx_t rewrites_applied = 0;
	bool verbose = true;
};

// ──────────────────────────────────────────────────────────────────────
// Expression fingerprinting: serialize an expression tree to a canonical
// string for structural comparison. Column references use binding info
// so that column identity is preserved.
// ──────────────────────────────────────────────────────────────────────
static string FingerprintExpression(const Expression &expr) {
	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		return "(" + ExpressionTypeToString(expr.type) + " " +
		       FingerprintExpression(*comp.left) + " " +
		       FingerprintExpression(*comp.right) + ")";
	}
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		// Sort children for canonical form (AND/OR are commutative)
		vector<string> child_fps;
		for (auto &child : conj.children) {
			child_fps.push_back(FingerprintExpression(*child));
		}
		sort(child_fps.begin(), child_fps.end());
		string result = "(" + ExpressionTypeToString(expr.type);
		for (auto &fp : child_fps) {
			result += " " + fp;
		}
		result += ")";
		return result;
	}
	case ExpressionType::VALUE_CONSTANT: {
		auto &constant = expr.Cast<BoundConstantExpression>();
		return "CONST:" + constant.value.ToString();
	}
	case ExpressionType::BOUND_COLUMN_REF: {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		// Use the alias (column name) for readability + matching
		return "COL:" + colref.alias;
	}
	case ExpressionType::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		string result = "FN:" + func.function.name + "(";
		for (idx_t i = 0; i < func.children.size(); i++) {
			if (i > 0) result += ",";
			result += FingerprintExpression(*func.children[i]);
		}
		result += ")";
		return result;
	}
	default:
		return "EXPR:" + expr.ToString();
	}
}

// ──────────────────────────────────────────────────────────────────────
// Collect all filter expressions from a LogicalFilter node.
// Handles conjunction (AND) by flattening.
// ──────────────────────────────────────────────────────────────────────
static void CollectFilterExpressions(const vector<unique_ptr<Expression>> &expressions,
                                      vector<string> &fingerprints) {
	for (auto &expr : expressions) {
		fingerprints.push_back(FingerprintExpression(*expr));
	}
}

// ──────────────────────────────────────────────────────────────────────
// Get the table name from a LogicalGet operator
// ──────────────────────────────────────────────────────────────────────
static string GetTableName(const LogicalGet &get) {
	// The function name is typically "seq_scan" for table scans
	// The table name can be retrieved from the bind data
	return get.GetTable() ? get.GetTable()->name : get.function.name;
}

// ──────────────────────────────────────────────────────────────────────
// Core plan rewriter: recursively walks the logical plan tree
// looking for LogicalGet nodes with pushed-down filters that match
// registered rules. When a match is found, replaces the LogicalGet
// with a new LogicalGet that scans the pre-filtered replacement view.
// ──────────────────────────────────────────────────────────────────────

static LogicalOperator *FindLogicalGet(LogicalOperator *op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return op;
	}
	for (auto &child : op->children) {
		auto *result = FindLogicalGet(child.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

static unique_ptr<LogicalOperator> ExtractLogicalGet(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		return std::move(op);
	}
	for (auto &child : op->children) {
		auto result = ExtractLogicalGet(child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

static void CollectPushedDownFilters(TableFilterSet &filters, vector<string> &fps) {
	for (auto &entry : filters.filters) {
		auto column_index = entry.first;
		auto &filter = entry.second;
		// Build a canonical string representation of the filter
		// e.g., "col_3 >= 5" or "col_7 = 'BUILDING'"
		string fp = filter->ToString("col_" + to_string(column_index));
		fps.push_back(fp);
	}
}

static void RewritePlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
                        ViewRewriterInfo *info) {
	// Recurse into children first (bottom-up)
	for (auto &child : op->children) {
		RewritePlan(context, child, info);
	}

	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = op->Cast<LogicalGet>();

	string table_name;
	try {
		table_name = GetTableName(get);
	} catch (...) {
		return;
	}

	if (table_name.empty()) {
		return;
	}

	// Build fingerprint from the pushed-down filters on this GET node
	vector<string> filter_fps;
    CollectPushedDownFilters(get.table_filters, filter_fps);

	// TODO: Maybe we also need to check get.expressions

	if (filter_fps.empty()) {
		return;
	}

	sort(filter_fps.begin(), filter_fps.end());

	string combined_fp;
	for (auto &fp : filter_fps) {
		if (!combined_fp.empty()) combined_fp += " AND ";
		combined_fp += fp;
	}

	if (info->verbose) {
		Printer::Print("ViewRewriter: Examining GET on table '" + table_name +
		               "' with pushed-down filters");
		Printer::Print("  Fingerprint: " + combined_fp);
	}

	// Check against registered rules
	for (auto &rule : info->rules) {
		if (!StringUtil::CIEquals(rule.source_table, table_name)) {
			continue;
		}
		if (rule.filter_fingerprint != combined_fp) {
			continue;
		}

		if (info->verbose) {
			Printer::Print("  MATCH! Rewriting to view '" + rule.replacement_view + "'");
		}

		try {
			// Look up the replacement view in the catalog
			auto &catalog = get.GetTable()->catalog;
			string schema_name = get.GetTable()->GetInfo()->schema;

			auto replacement_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, 
                schema_name, rule.replacement_view, OnEntryNotFound::RETURN_NULL);

			if (!replacement_entry) {
				if (info->verbose) {
					Printer::Print("  WARNING: Replacement '" + rule.replacement_view +
					               "' not found, skipping.");
				}
				continue;
			}

			// Get the table's scan function and storage
			auto &storage = replacement_entry.GetStorage();
			auto &table_function = storage.GetScanFunction();

			// Build the column name -> index mapping for the replacement table
			auto &rep_columns = replacement_entry.GetColumns();
			unordered_map<string, idx_t> rep_column_map;
			for (idx_t i = 0; i < rep_columns.LogicalColumnCount(); i++) {
				auto &col = rep_columns.GetColumn(LogicalIndex(i));
				rep_column_map[StringUtil::Lower(col.GetName())] = i;
			}

			// Remap: for each column the original GET reads, find it in the replacement
			vector<LogicalType> new_types;
			vector<string> new_names;
			vector<idx_t> new_column_ids;
			for (idx_t i = 0; i < get.names.size(); i++) {
				auto it = rep_column_map.find(StringUtil::Lower(get.names[i]));
				if (it == rep_column_map.end()) {
					throw InternalException("Replacement table '" + rule.replacement_view +
					                        "' is missing column '" + get.names[i] + "'");
				}
				new_column_ids.push_back(it->second);
				new_names.push_back(get.names[i]);
				new_types.push_back(get.returned_types[i]);
			}

			// Create bind data for the replacement table scan
			vector<LogicalType> return_types;
			for (idx_t i = 0; i < rep_columns.LogicalColumnCount(); i++) {
				return_types.push_back(rep_columns.GetColumn(LogicalIndex(i)).GetType());
			}
			auto bind_data = storage.GetBind();

            // TODO: We need to bind the replacement table and also put the correct index

			// Construct the new LogicalGet directly
			auto new_get = make_uniq<LogicalGet>(
			    get.table_index, table_function, std::move(bind_data),
			    get.types, get.names); // TODO: Virtual columns

			new_get->column_ids = std::move(new_column_ids);
			new_get->returned_types = get.returned_types;

			// No pushed-down filters — the replacement table's data already
			// reflects the filter semantics we matched on
			new_get->table_filters = nullptr;

			// Preserve the original table_index (already set via constructor)
			// so parent operators' ColumnBindings remain valid

			// Replace the operator in-place
			op = std::move(new_get);

			info->rewrites_applied++;

			if (info->verbose) {
				Printer::Print("  Rewrite applied: replaced scan of '" + table_name +
				               "' with scan of view '" + rule.replacement_view + "'");
			}
			return;

		} catch (std::exception &e) {
			if (info->verbose) {
				Printer::Print("  ERROR during rewrite: " + string(e.what()));
			}
		}
	}
}

// ──────────────────────────────────────────────────────────────────────
// The optimizer hook function called after DuckDB's built-in optimizers
// ──────────────────────────────────────────────────────────────────────
static void ViewRewriterOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto info = dynamic_cast<ViewRewriterInfo *>(input.info.get());
	if (!info) {
		return;
	}
	RewritePlan(input.context, plan, info);
}

// ──────────────────────────────────────────────────────────────────────
// Table function: view_rewriter_add_rule(source_table, filter_fingerprint, replacement_view)
// Registers a new rewrite rule.
// ──────────────────────────────────────────────────────────────────────
struct AddRuleBindData : public TableFunctionData {
	string message;
};

static unique_ptr<FunctionData> AddRuleBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
	if (input.inputs.size() != 3) {
		throw BinderException("tpch_rewriter_add_rule requires 3 arguments: "
		                      "source_table, filter_fingerprint, replacement_view");
	}

	auto source_table = input.inputs[0].GetValue<string>();
	auto filter_fingerprint = input.inputs[1].GetValue<string>();
	auto replacement_view = input.inputs[2].GetValue<string>();

	// Find our optimizer extension info
	auto &config = DBConfig::GetConfig(context);
	for (auto &ext : config.optimizer_extensions) {
		auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
		if (info) {
			RewriteRule rule;
			rule.source_table = source_table;
			rule.filter_fingerprint = filter_fingerprint;
			rule.replacement_view = replacement_view;
			rule.filter_description = filter_fingerprint;
			info->rules.push_back(std::move(rule));
			break;
		}
	}

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");

	auto bind_data = make_uniq<AddRuleBindData>();
	bind_data->message = "Rule added: " + source_table + " -> " + replacement_view;
	return std::move(bind_data);
}

static void AddRuleFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	// This is a no-op scan function; the work is done in bind
}

// ──────────────────────────────────────────────────────────────────────
// Table function: view_rewriter_capture_fingerprint(query)
// Parses and plans a query, then extracts filter fingerprints from
// the optimized plan. This lets users discover what fingerprint
// string to use when registering rules.
// ──────────────────────────────────────────────────────────────────────
struct CaptureBindData : public TableFunctionData {
	string query;
	std::vector<std::tuple<string, string, string>> results; // table, fingerprint, filter_text
	idx_t offset = 0;
};

static void CollectFiltersFromPlan(LogicalOperator &op,
                                   vector<std::tuple<string, string, string>> &results) {
	for (auto &child : op.children) {
		CollectFiltersFromPlan(*child, results);
	}

	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (!filter.children.empty() &&
		    filter.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = filter.children[0]->Cast<LogicalGet>();
			string table_name;
			try {
				table_name = GetTableName(get);
			} catch (...) {
				table_name = "<unknown>";
			}

			vector<string> fps;
			CollectFilterExpressions(filter.expressions, fps);
			sort(fps.begin(), fps.end());

			string combined_fp;
			string combined_text;
			for (idx_t i = 0; i < fps.size(); i++) {
				if (i > 0) {
					combined_fp += " AND ";
					combined_text += " AND ";
				}
				combined_fp += fps[i];
				combined_text += filter.expressions[i]->ToString();
			}

			results.push_back({table_name, combined_fp, combined_text});
		}
	}
}

static unique_ptr<FunctionData> CaptureBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("table_name");
	names.push_back("filter_fingerprint");
	names.push_back("filter_text");

	auto bind_data = make_uniq<CaptureBindData>();

	if (!input.inputs.empty()) {
		bind_data->query = input.inputs[0].GetValue<string>();
	}

	return std::move(bind_data);
}

static void CaptureFunction(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CaptureBindData>();

	// Only run the capture on first call
	if (bind_data.offset == 0 && bind_data.results.empty() && !bind_data.query.empty()) {
		try {
			// Use the connection to plan the query
			auto &db = DatabaseInstance::GetDatabase(context);
			Connection con(db);

			// Plan the query to get the optimized logical plan
			auto prepared = con.Prepare(bind_data.query);
			if (prepared->HasError()) {
				bind_data.results.push_back({"ERROR", prepared->GetError(), ""});
			} else {
				// Execute with EXPLAIN to see the plan
				auto result = con.Query("EXPLAIN " + bind_data.query);
				if (result->HasError()) {
					bind_data.results.push_back({"ERROR", result->GetError(), ""});
				} else {
					// We need the actual logical plan - let's get it via the prepared stmt
					// For now, output the EXPLAIN text as guidance
					string plan_text;
					while (true) {
						auto chunk = result->Fetch();
						if (!chunk || chunk->size() == 0) break;
						for (idx_t i = 0; i < chunk->size(); i++) {
							plan_text += chunk->GetValue(1, i).ToString() + "\n";
						}
					}
					bind_data.results.push_back({
					    "PLAN",
					    "See filter_text for the optimized plan. "
					    "Use tpch_rewriter_fingerprint_filter() for specific filters.",
					    plan_text
					});
				}
			}
		} catch (std::exception &e) {
			bind_data.results.push_back({"ERROR", e.what(), ""});
		}
	}

	idx_t count = 0;
	while (bind_data.offset < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
		auto &[tbl, fp, txt] = bind_data.results[bind_data.offset];
		output.SetValue(0, count, Value(tbl));
		output.SetValue(1, count, Value(fp));
		output.SetValue(2, count, Value(txt));
		bind_data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

// ──────────────────────────────────────────────────────────────────────
// Table function: view_rewriter_stats()
// Shows how many rewrites have been applied.
// ──────────────────────────────────────────────────────────────────────
struct StatsBindData : public TableFunctionData {
	bool done = false;
};

static unique_ptr<FunctionData> StatsBind(ClientContext &context,
                                          TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types,
                                          vector<string> &names) {
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("rules_registered");
	names.push_back("rewrites_applied");

	return make_uniq<StatsBindData>();
}

static void StatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<StatsBindData>();
	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t num_rules = 0;
    idx_t rewrites_applied = 0;
	auto &config = DBConfig::GetConfig(context);
	for (auto &ext : config.optimizer_extensions) {
		auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
		if (info) {
			num_rules        = info->rules.size();
            rewrites_applied = info->rewrites_applied;
			break;
		}
	}

	output.SetValue(0, 0, Value::BIGINT(num_rules));
	output.SetValue(1, 0, Value::BIGINT(rewrites_applied));
	output.SetCardinality(1);
	bind_data.done = true;
}

// ══════════════════════════════════════════════════════════════════════
// Extension Load
// ══════════════════════════════════════════════════════════════════════
static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
	// 1. Register the optimizer extension (runs AFTER built-in optimizers)
	auto &config = DBConfig::GetConfig(instance);

	OptimizerExtension opt_ext;
	opt_ext.optimize_function = ViewRewriterOptimize;
	opt_ext.optimizer_info = make_shared_ptr<ViewRewriterInfo>();
	config.optimizer_extensions.push_back(std::move(opt_ext));

	// 2. Register management table functions

	// view_rewriter_add_rule(source_table, filter_fingerprint, replacement_view)
	TableFunction add_rule_func("view_rewriter_add_rule",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    AddRuleFunction, AddRuleBind);
	loader.RegisterFunction(add_rule_func);

	// view_rewriter_capture_fingerprint(query)
	TableFunction capture_func("view_rewriter_capture_fingerprint",
	    {LogicalType::VARCHAR}, CaptureFunction, CaptureBind);
	loader.RegisterFunction(capture_func);

	// view_rewriter_stats()
	TableFunction stats_func("view_rewriter_stats", {}, StatsFunction, StatsBind);
	loader.RegisterFunction(stats_func);
}

void ViewRewriterExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ViewRewriterExtension::Name() {
	return "view_rewriter";
}

std::string ViewRewriterExtension::Version() const {
#ifdef EXT_VERSION_VIEW_REWRITER
	return EXT_VERSION_VIEW_REWRITER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(view_rewriter, loader) {
	duckdb::LoadInternal(loader);
}

}
