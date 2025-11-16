#include "pretty.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void print_catalog(const system_catalog_t* catalog) {
  if (!catalog) {
    printf("Catalog is NULL\n");
    return;
  }

  printf("System Catalog:\n");
  printf("Tuple Size: %u bytes\n", catalog->tuple_size);
  printf("Record Count: %u\n", catalog->record_count);
  printf("Attributes:\n");
  for (uint8_t i = 0; i < catalog->record_count; i++) {
    const catalog_record_t* record = &catalog->records[i];
    printf("  Attribute %u:\n", i + 1);
    printf("    Name: %s\n", record->attribute_name);
    printf("    Size: %u bytes\n", record->attribute_size);
    printf("    Type: %s\n", attribute_type_to_string(record->attribute_type));
    printf("    Order: %u\n", record->attribute_order);
  }
}

void print_page(dbms_session_t* session, uint64_t page_id, bool print_nulls) {
  if (!session) {
    printf("DBMS session is NULL\n");
    return;
  }

  buffer_page_t* buffer_page = dbms_get_buffer_page(session, page_id);
  if (!buffer_page || !buffer_page->page) {
    printf("Page ID %llu not found in buffer pool\n", page_id);
    return;
  }

  page_t* page = buffer_page->page;
  printf("Page ID: %llu\n", page_id);
  printf("Next Page: %llu\n", page->next_page);
  printf("Previous Page: %llu\n", page->prev_page);
  printf("Free Space Head: %llu\n", page->free_space_head);
  printf("Tuples Per Page: %llu\n", page->tuples_per_page);
  printf("Is Dirty: %s\n", buffer_page->is_dirty ? "Yes" : "No");
  printf("Last Updated: %u\n", buffer_page->last_updated);
  printf("Tuples:\n");

  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  char* data = page->data;
  for (uint64_t i = 0; i < tuples_per_page; i++) {
    char* tuple_data = data + (i * session->catalog->tuple_size);
    tuple_t* tuple = &buffer_page->tuples[i];
    if (tuple->is_null) {
      if (print_nulls) {
        printf("NULL Tuple %llu (%llu, %llu):\n", i, tuple->id.page_id, tuple->id.slot_id);
        printf("  Next Free: %llu\n", *(uint64_t*)(tuple_data + FREE_POINTER_OFFSET));
      }
      continue;
    }

    print_tuple(session, tuple);
  }
}

void print_tuple(dbms_session_t* session, tuple_t* tuple) {
  if (!session || !tuple) {
    printf("Invalid session or tuple\n");
    return;
  }

  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);
  printf("Tuple (%llu, %llu):%s\n", tuple->id.page_id, tuple->id.slot_id, tuple->is_null ? " NULL" : "");
  if (tuple->is_null) {
    return;
  }

  for (uint8_t i = 0; i < num_attributes; i++) {
    catalog_record_t* record = dbms_get_catalog_record(session->catalog, i);
    if (record) {
      attribute_value_t* attr_value = &tuple->attributes[i];
      printf("  Attribute %u (%s): ", i + 1, record->attribute_name);
      switch (record->attribute_type) {
        case ATTRIBUTE_TYPE_INT:
          printf("%d\n", attr_value->int_value);
          break;
        case ATTRIBUTE_TYPE_FLOAT:
          printf("%f\n", attr_value->float_value);
          break;
        case ATTRIBUTE_TYPE_STRING:
          printf("%s\n", attr_value->string_value ? attr_value->string_value : "NULL");
          break;
        case ATTRIBUTE_TYPE_BOOL:
          printf("%s\n", attr_value->bool_value ? "true" : "false");
          break;
        default:
          printf("UNUSED\n");
          break;
      }
    }
  }
}

void print_query_result(query_result_t* result) {
  if (!result) {
    printf("Query result is NULL\n");
    return;
  }

  // Should print like this:
  // |---------------|---------------|---------------|
  // | Column1       | Column2       | Column3       |
  // |---------------|---------------|---------------|
  // | Value1        | Value2        | Value3        |
  // |---------------|---------------|---------------|

  // First need to find out the max width for each column
  // Numbers and bools are just 15 (QUERY_COLUMN_WIDTH)
  // Need to check strings for max length

  size_t* column_widths = calloc(result->column_count, sizeof(size_t));
  if (!column_widths) {
    printf("Memory allocation failed for column widths\n");
    return;
  }

  for (size_t j = 0; j < result->column_count; j++) {
    column_widths[j] = QUERY_COLUMN_WIDTH;  // Default width
    for (size_t i = 0; i < result->row_count; i++) {
      attribute_value_t* attr = &result->rows[i][j];
      if (attr->type == ATTRIBUTE_TYPE_STRING && attr->string_value) {
        size_t len = strlen(attr->string_value);
        if (len > column_widths[j]) {
          column_widths[j] = len;
        }
      }
    }

    // Also check column name length
    size_t col_name_len = strlen(result->column_names[j]);
    if (col_name_len > column_widths[j]) {
      column_widths[j] = col_name_len;
    }
  }

  // Calculate total width for separator line
  size_t total_width = 0;
  for (size_t j = 0; j < result->column_count; j++) {
    // 3 for "| " and space
    total_width += column_widths[j] + 3;
  }
  // For final "|"
  total_width += 1;

  // Define and print separator line
  char* separator_line = malloc(total_width + 1);
  if (!separator_line) {
    printf("Memory allocation failed for separator line\n");
    free(column_widths);
    return;
  }

  memset(separator_line, '-', total_width);
  size_t pos = 0;
  for (size_t j = 0; j < result->column_count; j++) {
    separator_line[pos] = '|';
    pos += column_widths[j] + 3;
  }
  separator_line[total_width - 1] = '|';
  separator_line[total_width] = '\0';

  printf("%s\n", separator_line);

  // Print header
  for (size_t j = 0; j < result->column_count; j++) {
    printf("| %-*s ", (int)column_widths[j], result->column_names[j]);
  }
  printf("|\n");

  printf("%s\n", separator_line);

  // Print rows
  for (size_t i = 0; i < result->row_count; i++) {
    for (size_t j = 0; j < result->column_count; j++) {
      attribute_value_t* attr = &result->rows[i][j];
      switch (attr->type) {
        case ATTRIBUTE_TYPE_INT:
          printf("| %-*d ", (int)column_widths[j], attr->int_value);
          break;
        case ATTRIBUTE_TYPE_FLOAT:
          printf("| %-*f ", (int)column_widths[j], attr->float_value);
          break;
        case ATTRIBUTE_TYPE_STRING:
          printf("| %-*s ", (int)column_widths[j], attr->string_value ? attr->string_value : "NULL");
          break;
        case ATTRIBUTE_TYPE_BOOL:
          printf("| %-*s ", (int)column_widths[j], attr->bool_value ? "true" : "false");
          break;
        default:
          printf("| %-*s ", (int)column_widths[j], "UNKNOWN");
          break;
      }
    }
    printf("|\n");
    printf("%s\n", separator_line);
  }

  free(column_widths);
  free(separator_line);
}

const char* attribute_type_to_string(uint8_t attribute_type) {
  switch (attribute_type) {
    case ATTRIBUTE_TYPE_INT:
      return "INT";
    case ATTRIBUTE_TYPE_FLOAT:
      return "FLOAT";
    case ATTRIBUTE_TYPE_STRING:
      return "STRING";
    case ATTRIBUTE_TYPE_BOOL:
      return "BOOL";
    default:
      return "UNUSED";
  }
}