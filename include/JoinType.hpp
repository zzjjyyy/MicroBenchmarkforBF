#pragma once

#include <assert.h>
#include <string>

enum JoinType {
  INNER,
  LEFT,
  RIGHT,
  FULL,
  LEFT_SEMI,
  RIGHT_SEMI
};

inline JoinType reverseJoinType(JoinType joinType) {
  switch (joinType) {
    case INNER:
    case FULL: 
        return joinType;
    case LEFT:
        return RIGHT;
    case RIGHT:
        return LEFT;
    case LEFT_SEMI:
        return RIGHT_SEMI;
    case RIGHT_SEMI:
        return LEFT_SEMI;
    default:
        assert(false);
  }
}

inline std::string joinTypeToStr(JoinType joinType) {
  switch (joinType) {
    case INNER:
        return "Inner";
    case LEFT:
        return "Left";
    case RIGHT:
        return "Right";
    case FULL:
        return "Full";
    case LEFT_SEMI:
        return "LeftSemi";
    case RIGHT_SEMI:
        return "RightSemi";
    default:
        assert(false);
  }
}

inline JoinType strToJoinType(const std::string &joinTypeStr) {
  if (joinTypeStr == "Inner") {
    return JoinType::INNER;
  } else if (joinTypeStr == "Left") {
    return JoinType::LEFT;
  } else if (joinTypeStr == "Right") {
    return JoinType::RIGHT;
  } else if (joinTypeStr == "Full") {
    return JoinType::FULL;
  } else if (joinTypeStr == "LeftSemi") {
    return JoinType::LEFT_SEMI;
  } else if (joinTypeStr == "RightSemi") {
    return JoinType::RIGHT_SEMI;
  } else {
    assert(false);
  }
}