#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <memory>
#include <vector>
#include "Column.hpp"
#include "TableHelper.hpp"
#include "TupleSetShowOptions.hpp"

/**
 * A list of tuples/rows/records. Really just encapsulates Arrow tables and record batches. Hiding
 * some of the rough edges.
 */
class TupleSet : public std::enable_shared_from_this<TupleSet> {
public:
  explicit TupleSet() = default;
  explicit TupleSet(std::shared_ptr<arrow::Table> table);

  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Table> &table);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::vector<std::shared_ptr<arrow::Array>>& values);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::vector<std::shared_ptr<arrow::ChunkedArray>>& values);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::vector<std::shared_ptr<Column>>& columns);
  static std::shared_ptr<TupleSet> make(const std::vector<std::shared_ptr<Column>>& columns);
  static std::shared_ptr<TupleSet> make(const std::vector<std::shared_ptr<arrow::RecordBatch>> &recordBatches);
  static std::shared_ptr<TupleSet> make(const std::shared_ptr<arrow::csv::TableReader> &tableReader);
  static std::shared_ptr<TupleSet> makeWithNullTable();
  static std::shared_ptr<TupleSet> makeWithEmptyTable();

  bool valid() const;
  bool validate() const;
  void clear();
  int64_t numRows() const;
  int numColumns() const;
  size_t size() const;
  std::shared_ptr<arrow::Schema> schema() const;
  std::shared_ptr<arrow::Table> table() const;
  void table(const std::shared_ptr<arrow::Table> &table);

  /**
   * Concatenate tupleSets.
   */
  static std::shared_ptr<TupleSet> concatenate(const std::vector<std::shared_ptr<TupleSet>>& tupleSets);

  /**
   * Append tupleSets.
   */
  void append(const std::vector<std::shared_ptr<TupleSet>>& tupleSet);
  void append(const std::shared_ptr<TupleSet>& tupleSet);

  /**
   * Get column.
   */
  std::shared_ptr<Column> getColumnByName(const std::string &name) const;
  std::shared_ptr<Column> getColumnByIndex(const int &columnIndex) const;

  /**
   * Project specified columns, ignore non-existing ones.
   * @param columnNames
   * @return
   */
  std::shared_ptr<TupleSet> projectExist(const std::vector<std::string> &columnNames) const;
  static std::shared_ptr<arrow::RecordBatch> projectExist(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                          const std::vector<std::string> &columnNames);
  static std::shared_ptr<arrow::RecordBatch> projectExist(const arrow::RecordBatch &recordBatch,
                                                          const std::vector<std::string> &columnNames);

  /**
   * Project specified columns
   * @param columnIds
   * @return
   */
  std::shared_ptr<TupleSet> project(const std::vector<int> &columnIds) const;

  /**
   * Rename columns.
   */
  void renameColumns(const std::vector<std::string>& columnNames);
  void renameColumns(const std::unordered_map<std::string, std::string> &columnRenames);
  std::shared_ptr<TupleSet> renameColumnsWithNewTupleSet(const std::vector<std::string>& columnNames);

  /**
   * Invokes combineChunks on the underlying table
   *
   * @return
   */
  void combine();

  /**
   * Returns the tuple set pretty printed as a string
   *
   * @return
   */
  std::string showString();
  std::string showString(TupleSetShowOptions options);

  /**
   * Returns a short string representing the tuple set
   * @return
   */
  std::string toString() const;

  /**
   * Perform a custom function which returns an scalar value on the table.
   * @param fn
   * @return
   */
  std::shared_ptr<arrow::Scalar> visit(const std::function<std::shared_ptr<arrow::Scalar>(
          std::shared_ptr<arrow::Scalar>, arrow::RecordBatch &)>& fn);

  /**
   * Returns an element from the tupleset given and column name and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  C_TYPE value(const std::string &columnName, int row) {
    return TableHelper::value<ARROW_TYPE, C_TYPE>(columnName, row, *table_);
  }

  /**
   * Returns an string element from the tupleset given and column name and row number.
   * @param columnName
   * @param row
   * @return
   */
  std::string stringValue(const std::string &columnName, int row){
	  return TableHelper::value<arrow::StringType, std::string>(columnName, row, *table_);
  }


  /**
   * Returns an element from the tupleset given and column number and row number.
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  C_TYPE value(int column, int row){
	return TableHelper::value<ARROW_TYPE, C_TYPE>(column, row, *table_);
  }

  /**
   * Returns a column from the tupleset as a vector given a column name
   *
   * @tparam ARROW_TYPE
   * @tparam C_TYPE
   * @param columnName
   * @param row
   * @return
   */
  template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
  std::shared_ptr<std::vector<C_TYPE>> vector(const std::string &columnName){
	return TableHelper::vector<ARROW_TYPE, C_TYPE>(*table_->GetColumnByName(columnName));
  }

private:
  static std::vector<std::shared_ptr<arrow::Table>>
  tupleSetVectorToArrowTableVector(const std::vector<std::shared_ptr<TupleSet>> &tupleSets);

  std::shared_ptr<arrow::Table> table_;
};

using TupleSetPtr = std::shared_ptr<TupleSet>;