#include "pretty.h"

#include <stdio.h>

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
  printf("Tuples:\n");

  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);
  char* data = page->data;
  for (uint64_t i = 0; i < tuples_per_page; i++) {
    char* tuple_data = data + (i * session->catalog->tuple_size);
    tuple_t* tuple = &buffer_page->tuples[i];
    if (tuple->is_null) {
      if (print_nulls) {
        printf("  NULL Tuple %llu (Page ID: %llu, Slot ID: %llu):\n", i, tuple->id.page_id, tuple->id.slot_id);
        printf("    Next Free: %llu\n", *(uint64_t*)(tuple_data + FREE_POINTER_OFFSET));
      }
      continue;
    }

    printf("  Tuple %llu (Page ID: %llu, Slot ID: %llu):\n", i, tuple->id.page_id, tuple->id.slot_id);
    for (uint8_t j = 0; j < num_attributes; j++) {
      catalog_record_t* record = dbms_get_catalog_record(session->catalog, j);
      if (record) {
        attribute_value_t* attr_value = &tuple->attributes[j];
        printf("    Attribute %u (%s): ", j + 1, record->attribute_name);
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