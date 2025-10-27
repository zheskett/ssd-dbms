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