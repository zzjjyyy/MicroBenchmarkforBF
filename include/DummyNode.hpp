#pragma once

#include <arrow/acero/exec_plan.h>

class DummyNode : public arrow::acero::ExecNode {
  DummyNode(arrow::acero::ExecPlan* plan, NodeVector inputs, bool is_sink,
            arrow::acero::StartProducingFunc start_producing, arrow::acero::StopProducingFunc stop_producing)
      : ExecNode(plan, std::move(inputs), {}, (is_sink) ? nullptr : dummy_schema()),
        start_producing_(std::move(start_producing)),
        stop_producing_(std::move(stop_producing)) {
    input_labels_.resize(inputs_.size());
    for (size_t i = 0; i < input_labels_.size(); ++i) {
      input_labels_[i] = std::to_string(i);
    }
  }

  const char* kind_name() const override { return "Dummy"; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override { return Status::OK(); }

  Status InputFinished(ExecNode* input, int total_batches) override {
    return Status::OK();
  }

  Status StartProducing() override {
    if (start_producing_) {
      RETURN_NOT_OK(start_producing_(this));
    }
    started_ = true;
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    ASSERT_NE(output_, nullptr) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    ASSERT_NE(output_, nullptr) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  Status StopProducingImpl() override {
    if (stop_producing_) {
      stop_producing_(this);
    }
    return Status::OK();
  }

 private:
  void AssertIsOutput(ExecNode* output) { ASSERT_EQ(output->output(), nullptr); }

  std::shared_ptr<Schema> dummy_schema() const {
    return schema({field("dummy", null())});
  }

  StartProducingFunc start_producing_;
  StopProducingFunc stop_producing_;
  std::unordered_set<ExecNode*> requested_stop_;
  bool started_ = false;
};