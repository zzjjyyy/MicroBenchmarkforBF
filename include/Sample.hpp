#pragma once

#include <memory>
#include <random>
#include <functional>
#include <vector>
#include "Arrays.hpp"
#include "TupleSet.hpp"

class Sample {
public:
  	/**
   	* 3 x 3 tuple set of strings
   	*
   	* @return
   	*/
  	[[maybe_unused]] static std::shared_ptr<TupleSet> sample3x3String();

  	static std::shared_ptr<Column> sample3String();

  	/**
   	* Creates a  numCols x numRows tuple set of random decimal strings between 0.0 and 100.0
   	*
   	* Each column is named "c_<column index>"
   	*
   	* @param numCols
   	* @param numRows
   	* @return
   	*/
  	template <typename CType, typename ArrowType>
  	static std::shared_ptr<TupleSet> sampleCxRInt(int numCols, int numRows, std::uniform_int_distribution<int> dist = std::uniform_int_distribution(0, 9)) {
		std::random_device rd;
		std::mt19937 gen(rd());
		return sampleCxR<CType, ArrowType>(numCols, numRows, [&]() -> auto { return dist(gen); });
  	}

  	template <typename CType, typename ArrowType>
  	[[maybe_unused]] static std::shared_ptr<TupleSet> sampleCxRReal(int numCols, int numRows, std::uniform_real_distribution<double> dist = std::uniform_real_distribution(0.0, 9.0)) {
		std::random_device rd;
		std::mt19937 gen(rd());
		return sampleCxR<CType, ArrowType>(numCols, numRows, [&]() -> auto { return dist(gen); });
  	}

  	template <typename CType, typename ArrowType>
  	static std::shared_ptr<TupleSet> sampleCxR(int numCols, int numRows, const std::function<CType()> &valueGenerator) {
		std::vector<std::vector<CType>> data;
		for (int c = 0; c < numCols; ++c) {
	  		std::vector<CType> row;
	  		row.reserve(numRows);
	  		for (int r = 0; r < numRows; ++r) {
				row.emplace_back(valueGenerator());
	  		}
	  		data.emplace_back(row);
		}

		std::vector<std::shared_ptr<arrow::Field>> fields;
		fields.reserve(numCols);
		for (int c = 0; c < numCols; ++c) {
			char c_str[15];
			sprintf(c_str, "c_%d", c);
			std::string str = c_str;
	  		fields.emplace_back(field(str, arrow::TypeTraits<ArrowType>::type_singleton()));
		}

		auto arrowSchema = arrow::schema(fields);

		std::vector<std::shared_ptr<arrow::Array>> arrays;
		arrays.reserve(numCols);
		for (int c = 0; c < numCols; ++c) {
	  		arrays.emplace_back(Arrays::make<ArrowType>(data[c]));
		}
		std::vector<std::shared_ptr<Column>> columns;
		columns.reserve(numCols);
		for (int c = 0; c < numCols; ++c) {
	  		columns.emplace_back(Column::make(fields[c]->name(), arrays[c]));
		}
		auto tupleSet = TupleSet::make(arrowSchema, columns);
		return tupleSet;
  	}
};