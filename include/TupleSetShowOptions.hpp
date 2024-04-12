#pragma once

enum TupleSetShowOrientation {
  ColumnOriented,
  RowOriented
};

class TupleSetShowOptions {
public:
  explicit TupleSetShowOptions(TupleSetShowOrientation  Orientation, int maxNumRows = 10);
  [[nodiscard]] TupleSetShowOrientation getOrientation() const;
  [[nodiscard]] int getMaxNumRows() const;
private:
  TupleSetShowOrientation orientation_;
  int maxNumRows_;
};