#pragma once

#include <memory>

#include "ArrayHelper.hpp"
#include "ColumnName.hpp"

/**
 * Bunch of utilities for working with Arrow tables
 *
 */
class TableHelper {
public:

    /**
    * Returns an element from the array using the given index.
    *
    * TODO: It's not clear how to find which chunk the row is in, so we slice out just that row and then
    *  get the first chunk from the slice. It works but is this the best way to get the data out?
    *
    * NOTE: This is really just for testing, its not the most efficient way to get elements.
    *
    * FIXME: This should go in the chunked array helper
    *
    * @tparam ARROW_TYPE
    * @tparam C_TYPE
    * @param array
    * @param row
    * @return
    */
    template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
    static C_TYPE value(arrow::ChunkedArray &array, int index) {
	    // Check index
	    if (index > array.length())
	        assert(false);
	    auto slice = array.Slice(index, 1);
	    auto sliceChunk = slice->chunk(0);
	    using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<ARROW_TYPE>::ArrayType;
	    auto &typedArray = dynamic_cast<ARROW_ARRAY_TYPE &>(*sliceChunk);
	    return ArrayHelper<ARROW_ARRAY_TYPE, C_TYPE>::at(typedArray, 0);
    }

    /**
    * Returns an element from the table given a column number and row number.
    *
    * @tparam ARROW_TYPE
    * @tparam C_TYPE
    * @param columnName
    * @param row
    * @param table
    * @return
    */
    template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
    static C_TYPE value(const std::string &columnName, int row, const arrow::Table &table) {
        auto canonicalColumnName = ColumnName::canonicalize(columnName);
	    // Check column name
	    auto array = table.GetColumnByName(canonicalColumnName);
	    if (!array)
	        assert(false);
	    return value<ARROW_TYPE, C_TYPE>(*array, row);
    }

    /**
    * Returns an element from the table given a column number and row number.
    *
    * @tparam ARROW_TYPE
    * @tparam C_TYPE
    * @param col
    * @param row
    * @param table
    * @return
    */
    template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
    static C_TYPE value(int col, int row, const arrow::Table &table) {
	    // Check column index
	    auto array = table.column(col);
	    if (!array)
	        return assert(false);
	    return value<ARROW_TYPE, C_TYPE>(*array, row);
    }

    template<typename ARROW_TYPE, typename C_TYPE = typename ARROW_TYPE::c_type>
    static std::shared_ptr<std::vector<C_TYPE>> vector(arrow::ChunkedArray &array) {
        auto vector = std::make_shared<std::vector<C_TYPE>>();
	    using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<ARROW_TYPE>::ArrayType;
        for(const auto& chunk: array.chunks()){
	        auto &typedChunk = dynamic_cast<ARROW_ARRAY_TYPE &>(*chunk);
	        for (int i = 0; i < typedChunk.length(); ++i) {
		        vector->push_back(typedChunk.Value(i));
	        }
        }
	    return vector;
    }
};