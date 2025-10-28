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

// TODO: Create buffer pool
#define BUFFER_POOL_SIZE 1

#define PADDING_NAME "PADDING"

typedef struct {
  uint64_t next_page;
  uint64_t prev_page;
  uint64_t free_space_head;
  uint64_t tuples_per_page;
  char data[DATA_SIZE];
} page_t;

typedef struct {
  uint64_t page_id;
  uint64_t slot_id;
} tuple_id_t;

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

typedef struct {
  uint8_t type;
  union {
    int32_t int_value;
    float float_value;
    char* string_value;
    bool bool_value;
  };
} attribute_value_t;

typedef struct {
  tuple_id_t id;
  attribute_value_t* attributes;
} tuple_t;

typedef struct {
  bool is_free;
  bool is_dirty;
  uint32_t order_added;
  uint64_t page_id;
  page_t* page;
  tuple_t* tuples;
} buffer_page_t;

typedef struct {
  uint32_t page_count;
  buffer_page_t buffer_pages[BUFFER_POOL_SIZE];
} buffer_pool_t;

typedef struct {
  int fd;
  char* table_name;
  system_catalog_t* catalog;
  buffer_pool_t* buffer_pool;
} dbms_session_t;

/**
 * @brief Creates a new database file with the given system catalog
 *
 * @param filename Name of the database file to create
 * @param catalog Pointer to the system catalog
 * @return true on success, false on failure
 */
bool dbms_create_db(const char* filename, const system_catalog_t* catalog);

/**
 * @brief Initializes a DBMS session
 *
 * @param filename Name of the database file to open
 * @return Pointer to the DBMS session on success, NULL on failure
 */
dbms_session_t* dbms_init_dbms_session(const char* filename);

/**
 * @brief Frees a DBMS session
 *
 * @param session Pointer to the DBMS session structure
 */
void dbms_free_dbms_session(dbms_session_t* session);

/**
 * @brief Frees the buffer pool structure
 *
 * @param pool Pointer to the buffer pool to free
 */
void dbms_free_buffer_pool(buffer_pool_t* pool);

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

/**
 * @brief Returns the number of used (non-padding) attributes in the catalog
 *
 * @param catalog Pointer to the system catalog
 * @return Number of used attributes
 */
uint8_t dbms_catalog_num_used(const system_catalog_t* catalog);

/**
 * @brief Returns the number of tuples that can fit in a page based on the catalog
 *
 * @param catalog Pointer to the system catalog
 * @return Number of tuples per page
 */
uint64_t dbms_catalog_tuples_per_page(const system_catalog_t* catalog);
#endif /* DBMS_H */