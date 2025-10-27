#ifndef DBMS_H
#define DBMS_H

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#define PAGE_SIZE 8192
#define DATA_SIZE (PAGE_SIZE - 32)
#define NULL_BYTE_SIZE 1
#define FREE_POINTER_OFFSET (NULL_BYTE_SIZE * sizeof(uint64_t))

#define CATALOG_RECORD_SIZE 64
#define CATALOG_ATTRIBUTE_NAME_SIZE (CATALOG_RECORD_SIZE - 3)

#define ATTRIBUTE_TYPE_UNUSED 0
#define ATTRIBUTE_TYPE_INT 1
#define ATTRIBUTE_TYPE_FLOAT 2
#define ATTRIBUTE_TYPE_STRING 3
#define ATTRIBUTE_TYPE_BOOL 4

#define PADDING_NAME "PADDING"

typedef struct page {
  uint64_t next_page;
  uint64_t prev_page;
  uint64_t free_space_head;
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
 * @note Only use on heap allocated catalogs
 */
void dbms_free_system_catalog(system_catalog_t* catalog);

/**
 * @brief Frees the catalog records within a system catalog
 *
 * @param catalog The system catalog whose records are to be freed
 */
void dbms_free_catalog_records(system_catalog_t catalog);

/**
 * @brief Calculates the byte offset of an attribute within a tuple
 *
 * @param catalog Pointer to the system catalog
 * @param attribute_position Position of the attribute (0-based index)
 * @return Byte offset of the attribute within the tuple
 */
off_t dbms_get_attribute_offset(const system_catalog_t* catalog, uint8_t attribute_position);

/**
 * @brief Retrieves a catalog record by attribute position
 *
 * @param catalog Pointer to the system catalog
 * @param attribute_position Position of the attribute (0-based index)
 * @return Pointer to the catalog record, or NULL if not found
 */
catalog_record_t* dbms_get_catalog_record(const system_catalog_t* catalog, uint8_t attribute_position);

/**
 * @brief Retrieves a catalog record by attribute name
 *
 * @param catalog Pointer to the system catalog
 * @param attribute_name Name of the attribute to find
 * @return Pointer to the catalog record, or NULL if not found
 */
catalog_record_t* dbms_get_catalog_record_by_name(const system_catalog_t* catalog, const char* attribute_name);

/**
 * @brief Initializes a page structure based on the system catalog
 *
 * @param catalog Pointer to the system catalog
 * @param page Pointer to the page to initialize
 * @return true on success, false on failure
 */
bool dbms_init_page(const system_catalog_t* catalog, page_t* page);

#endif /* DBMS_H */