#include "query.h"

#include <stdio.h>
#include <stdlib.h>

static query_result_t* allocate_query_result(size_t column_count);
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
    buffer_page_t* buffer_page = dbms_get_buffer_page(session, page_id);
    if (!buffer_page) {
      fprintf(stderr, "Failed to retrieve buffer page for page ID %llu\n", page_id);
      query_free_query_result(result);
      return NULL;
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
      // BUG: attribute string values are not null-terminated
      return strcmp(attribute->string_value, value->string_value) == 0;
    case ATTRIBUTE_TYPE_BOOL:
      return attribute->bool_value == value->bool_value;
    default:
      return false;
  }
}