#include "executor/executor.h"

#include <stdlib.h>

void operator_free(Operator* op) {
  if (!op) {
    return;
  }

  // Free children recursively
  if (op->children) {
    for (int i = 0; i < op->child_count; i++) {
      operator_free(op->children[i]);
    }
    free(op->children);
  }

  // Call operator-specific destroy function to clean up internal allocations
  if (op->destroy) {
    op->destroy(op);
  }

  // Free the operator's private state struct
  if (op->state) {
    free(op->state);
  }

  free(op);
}

