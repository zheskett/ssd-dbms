#include "executor/nested_loop_join.h"

#include <stdlib.h>

// Forward declarations for iterator interface
static void nested_loop_join_open(Operator* self);
static tuple_t* nested_loop_join_next(Operator* self);
static void nested_loop_join_close(Operator* self);
static void nested_loop_join_reset(Operator* self);
static void nested_loop_join_destroy(Operator* self);

Operator* nested_loop_join_create(Operator* outer, Operator* inner,
                                  dbms_session_t* session,
                                  uint8_t outer_column_count,
                                  uint8_t inner_column_count) {
    if (!outer || !inner || !session) {
        return NULL;
    }

    Operator* op = calloc(1, sizeof(Operator));
    if (!op) {
        return NULL;
    }

    NestedLoopJoinState* state = calloc(1, sizeof(NestedLoopJoinState));
    if (!state) {
        free(op);
        return NULL;
    }

    state->session = session;
    state->outer_tuple = NULL;
    state->outer_exhausted = false;
    state->outer_attr_count = outer_column_count;
    state->inner_attr_count = inner_column_count;

    // Allocate combined attributes array
    uint8_t total_attrs = outer_column_count + inner_column_count;
    state->combined_attrs = calloc(total_attrs, sizeof(attribute_value_t));
    if (!state->combined_attrs) {
        free(state);
        free(op);
        return NULL;
    }

    // Initialize combined tuple
    state->combined_tuple.id.page_id = 0;
    state->combined_tuple.id.slot_id = 0;
    state->combined_tuple.is_null = false;
    state->combined_tuple.attributes = state->combined_attrs;

    op->state = state;
    op->open = nested_loop_join_open;
    op->next = nested_loop_join_next;
    op->close = nested_loop_join_close;
    op->reset = nested_loop_join_reset;
    op->destroy = nested_loop_join_destroy;

    // Set up children: outer = children[0], inner = children[1]
    op->children = calloc(2, sizeof(Operator*));
    if (!op->children) {
        free(state->combined_attrs);
        free(state);
        free(op);
        return NULL;
    }
    op->children[0] = outer;
    op->children[1] = inner;
    op->child_count = 2;

    return op;
}

static void nested_loop_join_open(Operator* self) {
    if (!self || !self->state || !self->children || self->child_count < 2) {
        return;
    }

    NestedLoopJoinState* state = (NestedLoopJoinState*)self->state;
    Operator* outer = self->children[0];
    Operator* inner = self->children[1];

    // Open both children
    if (outer && outer->open) {
        outer->open(outer);
    }
    if (inner && inner->open) {
        inner->open(inner);
    }

    // Get first outer tuple
    state->outer_exhausted = false;
    if (outer && outer->next) {
        state->outer_tuple = outer->next(outer);
        if (!state->outer_tuple) {
            state->outer_exhausted = true;
        }
    }
}

static tuple_t* nested_loop_join_next(Operator* self) {
    if (!self || !self->state || !self->children || self->child_count < 2) {
        return NULL;
    }

    NestedLoopJoinState* state = (NestedLoopJoinState*)self->state;
    Operator* outer = self->children[0];
    Operator* inner = self->children[1];

    // If outer is exhausted, no more results
    if (state->outer_exhausted || !state->outer_tuple) {
        return NULL;
    }

    while (true) {
        // Try to get next inner tuple
        tuple_t* inner_tuple = NULL;
        if (inner && inner->next) {
            inner_tuple = inner->next(inner);
        }

        if (inner_tuple) {
            // Combine outer + inner tuples (shallow copy)
            for (uint8_t i = 0; i < state->outer_attr_count; i++) {
                state->combined_attrs[i] = state->outer_tuple->attributes[i];
            }
            for (uint8_t i = 0; i < state->inner_attr_count; i++) {
                state->combined_attrs[state->outer_attr_count + i] = inner_tuple->attributes[i];
            }

            // Use outer tuple's ID for the combined tuple
            state->combined_tuple.id = state->outer_tuple->id;
            state->combined_tuple.is_null = false;

            return &state->combined_tuple;
        }

        // Inner exhausted - reset inner and advance outer
        if (inner && inner->reset) {
            inner->reset(inner);
        }

        // Get next outer tuple
        if (outer && outer->next) {
            state->outer_tuple = outer->next(outer);
        } else {
            state->outer_tuple = NULL;
        }

        if (!state->outer_tuple) {
            state->outer_exhausted = true;
            return NULL;
        }
    }
}

static void nested_loop_join_close(Operator* self) {
    if (!self || !self->state || !self->children || self->child_count < 2) {
        return;
    }

    NestedLoopJoinState* state = (NestedLoopJoinState*)self->state;
    Operator* outer = self->children[0];
    Operator* inner = self->children[1];

    // Close both children
    if (outer && outer->close) {
        outer->close(outer);
    }
    if (inner && inner->close) {
        inner->close(inner);
    }

    // Reset state
    state->outer_tuple = NULL;
    state->outer_exhausted = false;
}

static void nested_loop_join_reset(Operator* self) {
    if (!self || !self->state || !self->children || self->child_count < 2) {
        return;
    }

    NestedLoopJoinState* state = (NestedLoopJoinState*)self->state;
    Operator* outer = self->children[0];
    Operator* inner = self->children[1];

    // Reset both children
    if (outer && outer->reset) {
        outer->reset(outer);
    }
    if (inner && inner->reset) {
        inner->reset(inner);
    }

    // Get first outer tuple again
    state->outer_exhausted = false;
    if (outer && outer->next) {
        state->outer_tuple = outer->next(outer);
        if (!state->outer_tuple) {
            state->outer_exhausted = true;
        }
    }
}

static void nested_loop_join_destroy(Operator* self) {
    if (!self || !self->state) {
        return;
    }

    NestedLoopJoinState* state = (NestedLoopJoinState*)self->state;

    // Free combined attributes array
    if (state->combined_attrs) {
        free(state->combined_attrs);
        state->combined_attrs = NULL;
    }
}

