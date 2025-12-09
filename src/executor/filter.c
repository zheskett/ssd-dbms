#include "executor/filter.h"

#include <stdlib.h>
#include <string.h>

// Forward declarations for iterator interface
static void filter_open(Operator* self);
static tuple_t* filter_next(Operator* self);
static void filter_close(Operator* self);
static void filter_reset(Operator* self);

// Forward declarations for predicate evaluation
static bool evaluate_proposition(const attribute_value_t* attribute, const proposition_t* proposition);
static bool evaluate_criteria(const tuple_t* tuple, const selection_criteria_t* criteria);

Operator* filter_create(Operator* child, dbms_session_t* session, selection_criteria_t* criteria) {
  if (!child || !session) {
    return NULL;
  }

  Operator* op = calloc(1, sizeof(Operator));
  if (!op) {
    return NULL;
  }

  FilterState* state = calloc(1, sizeof(FilterState));
  if (!state) {
    free(op);
    return NULL;
  }

  state->session = session;
  state->criteria = criteria;

  op->state = state;
  op->open = filter_open;
  op->next = filter_next;
  op->close = filter_close;
  op->reset = filter_reset;
  op->destroy = NULL;  // criteria is not owned by this operator

  // Set up child relationship
  op->children = calloc(1, sizeof(Operator*));
  if (!op->children) {
    free(state);
    free(op);
    return NULL;
  }
  op->children[0] = child;
  op->child_count = 1;

  return op;
}

static void filter_open(Operator* self) {
  if (!self || !self->children || self->child_count < 1) {
    return;
  }

  // Open the child operator
  Operator* child = self->children[0];
  if (child && child->open) {
    child->open(child);
  }
}

static tuple_t* filter_next(Operator* self) {
  if (!self || !self->state || !self->children || self->child_count < 1) {
    return NULL;
  }

  FilterState* state = (FilterState*)self->state;
  Operator* child = self->children[0];

  if (!child || !child->next) {
    return NULL;
  }

  // Loop until we find a tuple that matches the criteria or reach end
  tuple_t* tuple;
  while ((tuple = child->next(child)) != NULL) {
    // If no criteria, pass all tuples through
    if (!state->criteria || state->criteria->proposition_count == 0) {
      return tuple;
    }

    // Evaluate criteria
    if (evaluate_criteria(tuple, state->criteria)) {
      return tuple;
    }
  }

  return NULL;
}

static void filter_close(Operator* self) {
  if (!self || !self->children || self->child_count < 1) {
    return;
  }

  // Close the child operator
  Operator* child = self->children[0];
  if (child && child->close) {
    child->close(child);
  }
}

static void filter_reset(Operator* self) {
  if (!self || !self->children || self->child_count < 1) {
    return;
  }

  // Simply reset the child operator
  Operator* child = self->children[0];
  if (child && child->reset) {
    child->reset(child);
  }
}

static bool evaluate_criteria(const tuple_t* tuple, const selection_criteria_t* criteria) {
  if (!tuple || !criteria) {
    return false;
  }

  // All propositions must match (AND semantics)
  for (size_t i = 0; i < criteria->proposition_count; i++) {
    const proposition_t* prop = &criteria->propositions[i];
    const attribute_value_t* attr = &tuple->attributes[prop->attribute_index];

    if (!evaluate_proposition(attr, prop)) {
      return false;
    }
  }

  return true;
}

static bool evaluate_proposition(const attribute_value_t* attribute, const proposition_t* proposition) {
  if (!attribute || !proposition) {
    return false;
  }

  const attribute_value_t* value = &proposition->value;

  switch (proposition->operator) {
    case OPERATOR_EQUAL:
      switch (attribute->type) {
        case ATTRIBUTE_TYPE_INT:
          return attribute->int_value == value->int_value;
        case ATTRIBUTE_TYPE_FLOAT:
          return attribute->float_value == value->float_value;
        case ATTRIBUTE_TYPE_STRING:
          return strcmp(attribute->string_value, value->string_value) == 0;
        case ATTRIBUTE_TYPE_BOOL:
          return attribute->bool_value == value->bool_value;
        default:
          return false;
      }

    case OPERATOR_NOT_EQUAL:
      switch (attribute->type) {
        case ATTRIBUTE_TYPE_INT:
          return attribute->int_value != value->int_value;
        case ATTRIBUTE_TYPE_FLOAT:
          return attribute->float_value != value->float_value;
        case ATTRIBUTE_TYPE_STRING:
          return strcmp(attribute->string_value, value->string_value) != 0;
        case ATTRIBUTE_TYPE_BOOL:
          return attribute->bool_value != value->bool_value;
        default:
          return false;
      }

    case OPERATOR_LESS_THAN:
      switch (attribute->type) {
        case ATTRIBUTE_TYPE_INT:
          return attribute->int_value < value->int_value;
        case ATTRIBUTE_TYPE_FLOAT:
          return attribute->float_value < value->float_value;
        case ATTRIBUTE_TYPE_STRING:
          return strcmp(attribute->string_value, value->string_value) < 0;
        default:
          return false;
      }

    case OPERATOR_LESS_EQUAL:
      switch (attribute->type) {
        case ATTRIBUTE_TYPE_INT:
          return attribute->int_value <= value->int_value;
        case ATTRIBUTE_TYPE_FLOAT:
          return attribute->float_value <= value->float_value;
        case ATTRIBUTE_TYPE_STRING:
          return strcmp(attribute->string_value, value->string_value) <= 0;
        default:
          return false;
      }

    case OPERATOR_GREATER_THAN:
      switch (attribute->type) {
        case ATTRIBUTE_TYPE_INT:
          return attribute->int_value > value->int_value;
        case ATTRIBUTE_TYPE_FLOAT:
          return attribute->float_value > value->float_value;
        case ATTRIBUTE_TYPE_STRING:
          return strcmp(attribute->string_value, value->string_value) > 0;
        default:
          return false;
      }

    case OPERATOR_GREATER_EQUAL:
      switch (attribute->type) {
        case ATTRIBUTE_TYPE_INT:
          return attribute->int_value >= value->int_value;
        case ATTRIBUTE_TYPE_FLOAT:
          return attribute->float_value >= value->float_value;
        case ATTRIBUTE_TYPE_STRING:
          return strcmp(attribute->string_value, value->string_value) >= 0;
        default:
          return false;
      }

    default:
      return false;
  }
}

