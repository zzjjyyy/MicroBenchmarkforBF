#include "TupleSetShowOptions.hpp"

TupleSetShowOptions::TupleSetShowOptions(TupleSetShowOrientation orientation, int maxNumRows)
    : orientation_(orientation), maxNumRows_(maxNumRows) {}

TupleSetShowOrientation TupleSetShowOptions::getOrientation() const {
    return orientation_;
}

int TupleSetShowOptions::getMaxNumRows() const {
    return maxNumRows_;
}