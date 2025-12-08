#ifndef DBMS_H
#define DBMS_H

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#include "data_structures.h"

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
#define BUFFER_POOL_SIZE 4

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
  bool is_null;
  attribute_value_t* attributes;
} tuple_t;

typedef struct {
  bool is_free;
  bool is_dirty;
  uint32_t pin_count;
  uint32_t last_updated;
  uint64_t page_id;
  page_t* page;
  tuple_t* tuples;
} buffer_page_t;

typedef struct {
  uint32_t page_count;
  hash_table_t* page_table;
  buffer_page_t buffer_pages[BUFFER_POOL_SIZE];
} buffer_pool_t;

typedef struct {
  int fd;
  uint32_t update_ctr;
  uint32_t page_count;
  char* table_name;
  char* filename;
  system_catalog_t* catalog;
  buffer_pool_t* buffer_pool;
} dbms_session_t;

typedef struct {
  dbms_session_t** sessions;
  size_t session_count;
} dbms_manager_t;

/**
 * @brief Creates a new database file with the given system catalog
 *
 * @param filename Name of the database file to create
 * @param catalog Pointer to the system catalog
 * @return true on success, false on failure
 */
bool dbms_create_table(const char* filename, const system_catalog_t* catalog);

/**
 * @brief Initializes the DBMS manager
 *
 * @return Pointer to the DBMS manager on success, NULL on failure
 */
dbms_manager_t* dbms_init_dbms_manager(void);

/**
 * @brief Frees the DBMS manager
 *
 * @param manager Pointer to the DBMS manager structure
 */
void dbms_free_dbms_manager(dbms_manager_t* manager);

/**
 * @brief Adds a DBMS session to the manager
 *
 * @param manager Pointer to the DBMS manager
 * @param session Pointer to the DBMS session to add
 * @return true on success, false on failure
 */
bool dbms_add_session(dbms_manager_t* manager, dbms_session_t* session);

/**
 * @brief Removes a DBMS session from the manager
 *
 * @param manager Pointer to the DBMS manager
 * @param session Pointer to the DBMS session to remove
 * @return true on success, false on failure
 */
bool dbms_remove_session(dbms_manager_t* manager, dbms_session_t* session);

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
 * @param catalog Pointer to the system catalog (for attribute sizes)
 * @param pool Pointer to the buffer pool to free
 */
void dbms_free_buffer_pool(const system_catalog_t* catalog, buffer_pool_t* pool);

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
 * @brief Retrieves a buffer page from the buffer pool by page ID
 *
 * If the page is not found in the buffer pool, it is loaded from disk.
 *
 * @param session Pointer to the DBMS session
 * @param page_id ID of the page to retrieve
 * @return Pointer to the buffer page, or NULL if not found
 */
buffer_page_t* dbms_get_buffer_page(dbms_session_t* session, uint64_t page_id);

/**
 * @brief Runs the buffer pool eviction policy to free up a buffer page
 * If there is a free page in the cache, that is the one that is returned.
 * If a page needs to be evicted, it is written back to disk if dirty.
 *
 * @param session Pointer to the DBMS session
 * @param target_index Pointer to store the index of the evicted page
 * @return Pointer to the evicted buffer page, or NULL on failure
 */
buffer_page_t* dbms_run_buffer_pool_policy(dbms_session_t* session, uint64_t* target_index);

/**
 * @brief Flushes a single buffer page to disk if it is dirty
 *
 * @param session Pointer to the DBMS session
 * @param buffer_page Pointer to the buffer page to flush
 * @param run_flush Whether to run the flush operation immediately
 */
void dbms_flush_buffer_page(dbms_session_t* session, buffer_page_t* buffer_page, bool run_flush);

/**
 * @brief Flushes all dirty pages in the buffer pool to disk
 *
 * @param session Pointer to the DBMS session
 */
void dbms_flush_buffer_pool(dbms_session_t* session);

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
 * @param page_id ID of the page to initialize
 * @return true on success, false on failure
 */
bool dbms_init_page(const system_catalog_t* catalog, page_t* page, uint64_t page_id);

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

/**
 * @brief Finds a buffer page with free space for inserting a tuple
 *
 * Reads from disk if necessary.
 *
 * @param session Pointer to the DBMS session
 * @return Pointer to the buffer page with free space, or NULL on failure
 */
buffer_page_t* dbms_find_page_with_free_space(dbms_session_t* session);

/**
 * @brief Inserts a tuple into the database
 *
 * @param session Pointer to the DBMS session
 * @param attributes Array of attribute values for the tuple.
 * Assumes correct number of attributes and correct order/types
 *
 * @return Pointer to the inserted tuple on success, NULL on failure
 */
tuple_t* dbms_insert_tuple(dbms_session_t* session, attribute_value_t* attributes);

/**
 * @brief Updates a tuple in the database
 *
 * @param session Pointer to the DBMS session
 * @param tuple_id The ID of the tuple to update
 * @param new_attributes Array of new attribute values for the tuple.
 * Assumes correct number of attributes and correct order/types
 *
 * @return Pointer to the updated tuple on success, NULL on failure
 */
tuple_t* dbms_update_tuple(dbms_session_t* session, tuple_id_t tuple_id, attribute_value_t* new_attributes);

/**
 * @brief Deletes a tuple by its tuple ID
 *
 * @param session Pointer to the DBMS session
 * @param tuple_id The ID of the tuple to delete
 * @return true on success, false on failure
 */
bool dbms_delete_tuple(dbms_session_t* session, tuple_id_t tuple_id);

/**
 * @brief Retrieves a tuple by its tuple ID
 *
 * @param session Pointer to the DBMS session
 * @param tuple_id The ID of the tuple to retrieve
 * @return Pointer to the tuple, or NULL if not found
 */
tuple_t* dbms_get_tuple(dbms_session_t* session, tuple_id_t tuple_id);

/**
 * @brief Pins a buffer page, incrementing its reference count.
 * If page is not in buffer, loads it from disk.
 *
 * @param session Pointer to the DBMS session
 * @param page_id ID of the page to pin
 * @return Pointer to the pinned buffer page, or NULL on failure
 */
buffer_page_t* dbms_pin_page(dbms_session_t* session, uint64_t page_id);

/**
 * @brief Unpins a buffer page, decrementing its reference count.
 * Page becomes eligible for eviction when pin_count reaches 0.
 *
 * @param session Pointer to the DBMS session
 * @param buffer_page Pointer to the buffer page to unpin
 */
void dbms_unpin_page(dbms_session_t* session, buffer_page_t* buffer_page);

/**
 * @brief Creates a deep copy of a tuple, allocating new string buffers.
 *
 * @param session The DBMS session (for catalog info)
 * @param src Source tuple to copy
 * @param dest Pre-allocated destination tuple (with pre-allocated attributes array)
 * @return true on success, false on failure
 */
bool dbms_copy_tuple(dbms_session_t* session, const tuple_t* src, tuple_t* dest);

#endif /* DBMS_H */
