#include "dbms.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void dbms_free_system_catalog(system_catalog_t* catalog) {
  if (catalog) {
    if (catalog->records) {
      free(catalog->records);
    }
    free(catalog);
  }
}

void dbms_free_catalog_records(system_catalog_t catalog) {
  if (catalog.records) {
    free(catalog.records);
  }
}

off_t dbms_get_attribute_offset(const system_catalog_t* catalog, uint8_t attribute_position) {
  if (!catalog || attribute_position >= catalog->record_count) {
    return -1;
  }

  // The first byte is reserved as a NULL byte
  off_t offset = NULL_BYTE_SIZE;
  for (uint8_t i = 0; i < attribute_position; i++) {
    offset += catalog->records[i].attribute_size;
  }
  return offset;
}

catalog_record_t* dbms_get_catalog_record(const system_catalog_t* catalog, uint8_t attribute_position) {
  if (!catalog || attribute_position >= catalog->record_count) {
    return NULL;
  }
  return &catalog->records[attribute_position];
}

catalog_record_t* dbms_get_catalog_record_by_name(const system_catalog_t* catalog, const char* attribute_name) {
  if (!catalog || !attribute_name) {
    return NULL;
  }
  for (uint8_t i = 0; i < catalog->record_count; i++) {
    if (strncmp(catalog->records[i].attribute_name, attribute_name, CATALOG_ATTRIBUTE_NAME_SIZE) == 0) {
      return &catalog->records[i];
    }
  }
  return NULL;
}

bool dbms_init_page(const system_catalog_t* catalog, page_t* page) {
  if (!catalog || !page) {
    return false;
  }

  // TODO: Multi page not added yet
  page->next_page = PAGE_SIZE * 2;
  page->prev_page = 0;

  page->free_space_head = PAGE_SIZE - DATA_SIZE;
  page->tuples_per_page = DATA_SIZE / catalog->tuple_size;
  if (page->tuples_per_page == 0) {
    free(page);
    fprintf(stderr, "Tuple size %u too large to fit in page\n", catalog->tuple_size);
    return false;
  }
  if (catalog->tuple_size % 8 != 0 || catalog->tuple_size < 16) {
    free(page);
    fprintf(stderr, "Tuple size %u is invalid\n", catalog->tuple_size);
    return false;
  }

  // For each tuple, add to free space linked list
  for (uint64_t i = 0; i < page->tuples_per_page; i++) {
    uint64_t tuple_offset = i * catalog->tuple_size + FREE_POINTER_OFFSET;
    uint64_t null_byte_offset = i * catalog->tuple_size;
    uint64_t* next_free_ptr = (uint64_t*)&page->data[tuple_offset];
    uint64_t* null_byte_ptr = (uint64_t*)&page->data[null_byte_offset];

    *null_byte_ptr = 0;  // Mark as free
    if (i == page->tuples_per_page - 1) {
      *next_free_ptr = 0;  // End of free list
    } else {
      // Don't add null byte size, should point to start of next tuple
      *next_free_ptr = tuple_offset + catalog->tuple_size - FREE_POINTER_OFFSET;
    }
  }

  return true;
}