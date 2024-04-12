#pragma once

#include <arrow/scalar.h>

class Scalar {
public:
    explicit Scalar(std::shared_ptr<arrow::Scalar> scalar);
    Scalar() = default;
    Scalar(const Scalar&) = default;
    Scalar& operator=(const Scalar&) = default;

    static std::shared_ptr<Scalar> make(const std::shared_ptr<::arrow::Scalar> &scalar);

    const std::shared_ptr<::arrow::Scalar> getArrowScalar() const;

    std::shared_ptr<::arrow::DataType> type();

    size_t hash();

    bool operator==(const Scalar &other) const;

    bool operator!=(const Scalar &other) const;

    std::string toString();

    template<typename T>
    T value() {
        if (scalar_->type->id() == ::arrow::Int32Type::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::Int32Scalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::Int64Type::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::Int64Scalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::FloatType::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::FloatScalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::DoubleType::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::DoubleScalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::Date64Type::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::Date64Scalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::BooleanType::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::BooleanScalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::StringType::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::StringScalar>(scalar_);
            auto typedValue = typedScalar->ToString();
            return typedValue;
        } else if (scalar_->type->id() == ::arrow::Decimal128Type::type_id) {
            auto typedScalar = std::static_pointer_cast<::arrow::Decimal128Scalar>(scalar_);
            auto typedValue = typedScalar->value;
            return typedValue;
        } else {
            assert(false);
        }
    }

private:
  std::shared_ptr<arrow::Scalar> scalar_;
};

using ScalarPtr = std::shared_ptr<Scalar>;