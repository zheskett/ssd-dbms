#ifndef DBMS_H
#define DBMS_H

#include <stdint.h>

#define PAGE_SIZE 8192
#define DATA_SIZE (PAGE_SIZE - 32)

#define CATALOG_RECORD_SIZE 64
#define CATALOG_ATTRIBUTE_NAME_SIZE (CATALOG_RECORD_SIZE - 3)

#define ATTRIBUTE_TYPE_UNUSED 0
#define ATTRIBUTE_TYPE_INT 1
#define ATTRIBUTE_TYPE_FLOAT 2
#define ATTRIBUTE_TYPE_STRING 3
#define ATTRIBUTE_TYPE_BOOL 4

#define PADDING_NAME "PADDING"

typedef struct page {
  struct page* next_page;
  struct page* prev_page;
  void* free_space_head;
  uint64_t tuples_per_page;
  char data[DATA_SIZE];
} page_t;

typedef struct {
  uint64_t page_id;
  uint64_t slot_id;
  char* data;
} tuple_t;

typedef struct {
  char attribute_name[CATALOG_ATTRIBUTE_NAME_SIZE];
  uint8_t attribute_size;
  uint8_t attribute_type;
  uint8_t attribute_order;
} catalog_record_t;

typedef struct {
  catalog_record_t* records;
  uint16_t tuple_size;
  uint8_t record_count;
} system_catalog_t;

/**
 * @brief Frees the system catalog structure
 *
 * @param catalog Pointer to the system catalog to free
 */
void dbms_free_system_catalog(system_catalog_t* catalog);

#endif /* DBMS_H */