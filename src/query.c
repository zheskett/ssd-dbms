#include "query.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static query_result_t* allocate_query_result(size_t column_count);
static void copy_attribute_values(attribute_value_t* dest, const attribute_value_t* src, size_t count);
static bool evaluate_proposition(const attribute_value_t* attribute, const proposition_t* proposition);
static bool check_operator_equal(const attribute_value_t* attribute, const attribute_value_t* value);
static bool check_operator_not_equal(const attribute_value_t* attribute, const attribute_value_t* value);
static bool check_operator_less_than(const attribute_value_t* attribute, const attribute_value_t* value);
static bool check_operator_less_equal(const attribute_value_t* attribute, const attribute_value_t* value);
static bool check_operator_greater_than(const attribute_value_t* attribute, const attribute_value_t* value);
static bool check_operator_greater_equal(const attribute_value_t* attribute, const attribute_value_t* value);

void query_free_query_result(query_result_t* result) {
  if (!result) {
    return;
  }

  if (result->rows) {
    for (size_t i = 0; i < result->row_count; i++) {
      if (!result->rows[i]) {
        continue;
      }

      for (size_t j = 0; j < result->column_count; j++) {
        attribute_value_t* attr = &result->rows[i][j];
        if (attr->type == ATTRIBUTE_TYPE_STRING && attr->string_value) {
          free(attr->string_value);
        }
      }
      free(result->rows[i]);
    }
    free(result->rows);
  }

  if (result->column_names) {
    for (size_t j = 0; j < result->column_count; j++) {
      if (result->column_names[j]) {
        free(result->column_names[j]);
      }
    }
    free(result->column_names);
  }
  free(result);
}

query_result_t* query_select(dbms_session_t* session, selection_criteria_t* criteria) {
  if (!session || !criteria) {
    return NULL;
  }

  // Allocate query result
  query_result_t* result = allocate_query_result(dbms_catalog_num_used(session->catalog));
  if (!result) {
    return NULL;
  }

  // Create column names from catalog
  for (uint8_t i = 0; i < result->column_count; i++) {
    catalog_record_t* record = dbms_get_catalog_record(session->catalog, i);
    if (!record) {
      fprintf(stderr, "Failed to retrieve catalog record for attribute index %u\n", i);
      query_free_query_result(result);
      return NULL;
    }
    result->column_names[i] = strndup(record->attribute_name, CATALOG_ATTRIBUTE_NAME_SIZE);
  }

  // Iterate through all tuples in the database and apply selection criteria
  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  for (uint64_t page_id = 0; page_id < session->page_count; page_id++) {
    for (uint64_t tuple_index = 0; tuple_index < tuples_per_page; tuple_index++) {
      tuple_t* tuple = dbms_get_tuple(session->catalog, (tuple_id_t){.page_id = page_id, .slot_id = tuple_index});
      // Skip null tuples
      if (!tuple) {
        continue;
      }

      // Evaluate selection criteria
      bool matches = true;
      for (size_t proposition_index = 0; proposition_index < criteria->proposition_count; proposition_index++) {
        proposition_t* proposition = &criteria->propositions[proposition_index];
        attribute_value_t* attribute = &tuple->attributes[proposition->attribute_index];
        if (!evaluate_proposition(attribute, proposition)) {
          matches = false;
          break;
        }
      }

      if (matches) {
        // Add tuple to result set
        attribute_value_t* result_row = calloc(result->column_count, sizeof(attribute_value_t));
        if (!result_row) {
          fprintf(stderr, "Memory allocation failed for query result row\n");
          query_free_query_result(result);
          return NULL;
        }

        copy_attribute_values(result_row, tuple->attributes, result->column_count);

        // Append row to result
        attribute_value_t** new_rows = realloc(result->rows, (result->row_count + 1) * sizeof(attribute_value_t*));
        if (!new_rows) {
          fprintf(stderr, "Memory allocation failed for expanding query result rows\n");
          free(result_row);
          query_free_query_result(result);
          return NULL;
        }
        result->rows = new_rows;
        result->rows[result->row_count++] = result_row;
      }
    }
  }

  return result;
}

static bool evaluate_proposition(const attribute_value_t* attribute, const proposition_t* proposition) {
  if (!attribute || !proposition) {
    return false;
  }

  switch (proposition->operator) {
    case OPERATOR_EQUAL:
      return check_operator_equal(attribute, &proposition->value);
    case OPERATOR_NOT_EQUAL:
      return check_operator_not_equal(attribute, &proposition->value);
    case OPERATOR_LESS_THAN:
      return check_operator_less_than(attribute, &proposition->value);
    case OPERATOR_LESS_EQUAL:
      return check_operator_less_equal(attribute, &proposition->value);
    case OPERATOR_GREATER_THAN:
      return check_operator_greater_than(attribute, &proposition->value);
    case OPERATOR_GREATER_EQUAL:
      return check_operator_greater_equal(attribute, &proposition->value);
    default:
      return false;
  }
}

static query_result_t* allocate_query_result(size_t column_count) {
  query_result_t* result = calloc(1, sizeof(query_result_t));
  if (!result) {
    fprintf(stderr, "Memory allocation failed for query result\n");
    return NULL;
  }
  result->column_count = column_count;
  result->column_names = calloc(column_count, sizeof(char*));
  if (!result->column_names) {
    fprintf(stderr, "Memory allocation failed for query result column names\n");
    free(result);
    return NULL;
  }
  return result;
}

static void copy_attribute_values(attribute_value_t* dest, const attribute_value_t* src, size_t count) {
  for (size_t i = 0; i < count; i++) {
    dest[i].type = src[i].type;
    switch (src[i].type) {
      case ATTRIBUTE_TYPE_INT:
        dest[i].int_value = src[i].int_value;
        break;
      case ATTRIBUTE_TYPE_FLOAT:
        dest[i].float_value = src[i].float_value;
        break;
      case ATTRIBUTE_TYPE_STRING:
        if (src[i].string_value) {
          dest[i].string_value = strdup(src[i].string_value);
        } else {
          dest[i].string_value = NULL;
        }
        break;
      case ATTRIBUTE_TYPE_BOOL:
        dest[i].bool_value = src[i].bool_value;
        break;
      default:
        break;
    }
  }
}

static bool check_operator_equal(const attribute_value_t* attribute, const attribute_value_t* value) {
  if (!attribute || !value || attribute->type != value->type) {
    return false;
  }

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
}

static bool check_operator_not_equal(const attribute_value_t* attribute, const attribute_value_t* value) {
  return !check_operator_equal(attribute, value);
}

static bool check_operator_less_than(const attribute_value_t* attribute, const attribute_value_t* value) {
  if (!attribute || !value || attribute->type != value->type) {
    return false;
  }

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
}

static bool check_operator_less_equal(const attribute_value_t* attribute, const attribute_value_t* value) {
  if (!attribute || !value || attribute->type != value->type) {
    return false;
  }

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
}

static bool check_operator_greater_than(const attribute_value_t* attribute, const attribute_value_t* value) {
  if (!attribute || !value || attribute->type != value->type) {
    return false;
  }

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
}

static bool check_operator_greater_equal(const attribute_value_t* attribute, const attribute_value_t* value) {
  if (!attribute || !value || attribute->type != value->type) {
    return false;
  }

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
}