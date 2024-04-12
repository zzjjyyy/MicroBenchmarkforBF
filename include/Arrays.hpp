#pragma once

#include <arrow/api.h>
#include <memory>

class Arrays {
public:
    /**
    * Creates a typed arrow array from the given vector
    *
    * @tparam TYPE
    * @tparam C_TYPE
    */
    template<typename TYPE, typename C_TYPE = typename TYPE::c_type>
    static std::shared_ptr<arrow::Array> make(std::vector<C_TYPE> values) {
        std::shared_ptr<arrow::Array> array;
        arrow::Status result;
        // Create the builder for the given vector type
        std::unique_ptr<arrow::ArrayBuilder> builder_ptr;
        result = arrow::MakeBuilder(arrow::default_memory_pool(), arrow::TypeTraits<TYPE>::type_singleton(), &builder_ptr);
        if(!result.ok())
            assert(false);
        auto &builder = dynamic_cast<typename arrow::TypeTraits<TYPE>::BuilderType &>(*builder_ptr);
        // Add the data
        for (size_t i = 0; i < values.size(); ++i) {
            result = builder.Append(values[i]);
            if(!result.ok())
                assert(false);
        }
        result = builder.Finish(&array);
        if(!result.ok())
            assert(false);
        return array;
    }
};