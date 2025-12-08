#include "executor/project.h"

#include <stdlib.h>
#include <string.h>

// Forward declarations for iterator interface
static void project_open(Operator* self);
static tuple_t* project_next(Operator* self);
static void project_close(Operator* self);
static void project_destroy(Operator* self);

Operator* project_create(Operator* child, dbms_session_t* session,
                         uint8_t* column_indices, uint8_t column_count) {
  if (!child || !session || !column_indices || column_count == 0) {
    return NULL;
  }

  Operator* op = calloc(1, sizeof(Operator));
  if (!op) {
    return NULL;
  }

  ProjectState* state = calloc(1, sizeof(ProjectState));
  if (!state) {
    free(op);
    return NULL;
  }

  state->session = session;
  state->column_count = column_count;

  // Copy the column indices array
  state->column_indices = calloc(column_count, sizeof(uint8_t));
  if (!state->column_indices) {
    free(state);
    free(op);
    return NULL;
  }
  memcpy(state->column_indices, column_indices, column_count * sizeof(uint8_t));

  // Allocate the projected attributes array
  state->projected_attrs = calloc(column_count, sizeof(attribute_value_t));
  if (!state->projected_attrs) {
    free(state->column_indices);
    free(state);
    free(op);
    return NULL;
  }

  // Initialize the projected tuple
  state->projected_tuple.id.page_id = 0;
  state->projected_tuple.id.slot_id = 0;
  state->projected_tuple.is_null = false;
  state->projected_tuple.attributes = state->projected_attrs;

  op->state = state;
  op->open = project_open;
  op->next = project_next;
  op->close = project_close;
  op->destroy = project_destroy;

  // Set up child relationship
  op->children = calloc(1, sizeof(Operator*));
  if (!op->children) {
    free(state->projected_attrs);
    free(state->column_indices);
    free(state);
    free(op);
    return NULL;
  }
  op->children[0] = child;
  op->child_count = 1;

  return op;
}

static void project_open(Operator* self) {
  if (!self || !self->children || self->child_count < 1) {
    return;
  }

  // Open the child operator
  Operator* child = self->children[0];
  if (child && child->open) {
    child->open(child);
  }
}

static tuple_t* project_next(Operator* self) {
  if (!self || !self->state || !self->children || self->child_count < 1) {
    return NULL;
  }

  ProjectState* state = (ProjectState*)self->state;
  Operator* child = self->children[0];

  if (!child || !child->next) {
    return NULL;
  }

  // Get the next tuple from the child
  tuple_t* tuple = child->next(child);
  if (!tuple) {
    return NULL;
  }

  // Copy tuple ID for reference
  state->projected_tuple.id = tuple->id;
  state->projected_tuple.is_null = tuple->is_null;

  // Project selected columns (shallow copy - pointers to buffer data)
  for (uint8_t i = 0; i < state->column_count; i++) {
    uint8_t src_idx = state->column_indices[i];
    // Copy the attribute value (for primitives this is a value copy,
    // for strings this copies the pointer - true zero-copy)
    state->projected_attrs[i] = tuple->attributes[src_idx];
  }

  return &state->projected_tuple;
}

static void project_close(Operator* self) {
  if (!self || !self->children || self->child_count < 1) {
    return;
  }

  // Close the child operator
  Operator* child = self->children[0];
  if (child && child->close) {
    child->close(child);
  }
}

static void project_destroy(Operator* self) {
  if (!self || !self->state) {
    return;
  }

  ProjectState* state = (ProjectState*)self->state;

  // Free allocated arrays within the state
  if (state->column_indices) {
    free(state->column_indices);
    state->column_indices = NULL;
  }

  if (state->projected_attrs) {
    free(state->projected_attrs);
    state->projected_attrs = NULL;
  }
}

