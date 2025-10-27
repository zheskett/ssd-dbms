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

void print_page(const page_t* page, const system_catalog_t* catalog, bool print_nulls) {
  if (!page || !catalog) {
    printf("Page or Catalog is NULL\n");
    return;
  }

  printf("Page Information:\n");
  printf("Next Page: %llu\n", page->next_page);
  printf("Previous Page: %llu\n", page->prev_page);
  printf("Tuples Per Page: %llu\n", page->tuples_per_page);
  printf("Free Space Head: %llu\n", page->free_space_head);

  // Print tuples
  for (uint64_t i = 0; i < page->tuples_per_page; i++) {
    const char* tuple_data = page->data + (i * catalog->tuple_size);
    // Check NULL byte
    if (tuple_data[0] != 0) {
      printf("Tuple %llu:\n", i + 1);

      for (uint8_t j = 0; j < catalog->record_count; j++) {
        const catalog_record_t* record = &catalog->records[j];
        const char* attribute_data = tuple_data + dbms_get_attribute_offset(catalog, j);
        printf("  %s: ", record->attribute_name);
        switch (record->attribute_type) {
          case ATTRIBUTE_TYPE_INT:
            printf("%d\n", *(int32_t*)attribute_data);
            break;
          case ATTRIBUTE_TYPE_FLOAT:
            printf("%f\n", *(float*)attribute_data);
            break;
          case ATTRIBUTE_TYPE_STRING:
            printf("%.*s\n", record->attribute_size, attribute_data);
            break;
          case ATTRIBUTE_TYPE_BOOL:
            printf("%s\n", (*(bool*)attribute_data) ? "true" : "false");
            break;
          default:
            break;
        }
      }
    } else if (print_nulls) {
      printf("Tuple %llu (NULL):\n", i + 1);
      printf("  Next Free: %llu\n", (*(uint64_t*)(tuple_data + FREE_POINTER_OFFSET)));
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