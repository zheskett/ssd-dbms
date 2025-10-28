#include "dbms.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ssdio.h"

bool dbms_create_db(const char* filename, const system_catalog_t* catalog) {
  if (!filename || !catalog) {
    return false;
  }

  int fd = ssdio_open(filename, true);
  if (fd == -1) {
    fprintf(stderr, "Failed to create database file: %s\n", filename);
    return false;
  }

  // Catalog is automatically aligned for O_DIRECT when called
  if (!ssdio_write_catalog(fd, catalog)) {
    fprintf(stderr, "Failed to write system catalog to database file\n");
    ssdio_close(fd);
    return false;
  }

  // Initialize first page
  // On linux, this has to be aligned to 4096 bytes
  // We just use the page size since it's a multiple of 4096
  page_t* first_page = aligned_alloc(PAGE_SIZE, sizeof(page_t));
  if (!first_page) {
    fprintf(stderr, "Memory allocation failed for first page\n");
    ssdio_close(fd);
    return false;
  }
  dbms_init_page(catalog, first_page);

  if (!ssdio_write_page(fd, 1, first_page)) {
    fprintf(stderr, "Failed to write first page to database file\n");
    free(first_page);
    ssdio_close(fd);
    return false;
  }

  ssdio_flush(fd);

  free(first_page);
  ssdio_close(fd);
  return true;
}

dbms_session_t* dbms_init_dbms_session(const char* filename) {
  if (!filename) {
    return NULL;
  }

  dbms_session_t* session = calloc(1, sizeof(dbms_session_t));
  if (!session) {
    fprintf(stderr, "Memory allocation failed for DBMS session\n");
    return NULL;
  }

  // Only keep ending filename as table name, remove path and extension
  const char* last_slash = strrchr(filename, '/');
  const char* base_name = last_slash ? last_slash + 1 : filename;
  const char* last_dot = strrchr(base_name, '.');
  size_t name_length = last_dot && (last_dot - base_name > 0) ? (size_t)(last_dot - base_name) : strlen(base_name);
  session->table_name = strndup(base_name, name_length);
  if (!session->table_name || strlen(session->table_name) == 0) {
    fprintf(stderr, "Validation failed for table name from filename: %s\n", filename);
    dbms_free_dbms_session(session);
    return NULL;
  }

  session->fd = ssdio_open(filename, false);
  if (session->fd == -1) {
    fprintf(stderr, "Failed to open database file: %s\n", filename);
    dbms_free_dbms_session(session);
    return NULL;
  }

  session->catalog = calloc(1, sizeof(system_catalog_t));

  if (!session->catalog) {
    fprintf(stderr, "Memory allocation failed for system catalog\n");
    dbms_free_dbms_session(session);
    return NULL;
  }

  if (!ssdio_read_catalog(session->fd, session->catalog)) {
    fprintf(stderr, "Failed to read system catalog from database file\n");
    dbms_free_dbms_session(session);
    return NULL;
  }

  session->buffer_pool = calloc(1, sizeof(buffer_pool_t));
  if (!session->buffer_pool) {
    fprintf(stderr, "Memory allocation failed for buffer pool\n");
    dbms_free_dbms_session(session);
    return NULL;
  }

  session->buffer_pool->page_count = 0;
  // Need to align pages for O_DIRECT
  page_t* pages = aligned_alloc(PAGE_SIZE, BUFFER_POOL_SIZE * sizeof(page_t));
  if (!pages) {
    fprintf(stderr, "Memory allocation failed for buffer pool pages\n");
    dbms_free_dbms_session(session);
    return NULL;
  }
  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    session->buffer_pool->buffer_pages[i].is_free = true;
    session->buffer_pool->buffer_pages[i].is_dirty = false;
    session->buffer_pool->buffer_pages[i].order_added = 0;
    session->buffer_pool->buffer_pages[i].page_id = 0;
    session->buffer_pool->buffer_pages[i].page = &pages[i];
  }

  // Allocate tuples for each buffer page
  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  tuple_t* all_tuples = calloc(BUFFER_POOL_SIZE * tuples_per_page, sizeof(tuple_t));
  if (!all_tuples) {
    fprintf(stderr, "Memory allocation failed for tuples\n");
    dbms_free_dbms_session(session);
    return NULL;
  }

  // Allocate attribute values for each tuple
  attribute_value_t* all_attribute_values =
      calloc(BUFFER_POOL_SIZE * tuples_per_page * dbms_catalog_num_used(session->catalog), sizeof(attribute_value_t));
  if (!all_attribute_values) {
    fprintf(stderr, "Memory allocation failed for attribute values\n");
    dbms_free_dbms_session(session);
    return NULL;
  }

  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    session->buffer_pool->buffer_pages[i].tuples = &all_tuples[i * tuples_per_page];
    for (uint64_t j = 0; j < tuples_per_page; j++) {
      tuple_t* tuple = &session->buffer_pool->buffer_pages[i].tuples[j];
      tuple->id.page_id = 0;
      tuple->id.slot_id = 0;
      tuple->attributes = &all_attribute_values[(i * tuples_per_page + j) * dbms_catalog_num_used(session->catalog)];
    }
  }

  return session;
}

void dbms_free_dbms_session(dbms_session_t* session) {
  if (session) {
    if (session->fd != -1) {
      ssdio_close(session->fd);
    }
    if (session->catalog) {
      dbms_free_system_catalog(session->catalog);
    }
    if (session->buffer_pool) {
      dbms_free_buffer_pool(session->buffer_pool);
    }
    if (session->table_name) {
      free(session->table_name);
    }
    free(session);
  }
}

void dbms_free_buffer_pool(buffer_pool_t* pool) {
  if (pool) {
    // Free each page in the buffer pages
    // They are allocated all together, so just free first one
    if (pool->buffer_pages[0].page) {
      free(pool->buffer_pages[0].page);
    }
    // Same for tuples/attribute values
    if (pool->buffer_pages[0].tuples) {
      if (pool->buffer_pages[0].tuples->attributes) {
        free(pool->buffer_pages[0].tuples->attributes);
      }
      free(pool->buffer_pages[0].tuples);
    }
    free(pool);
  }
}

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
  page->next_page = 0;
  page->prev_page = 0;

  page->free_space_head = PAGE_SIZE - DATA_SIZE;
  page->tuples_per_page = dbms_catalog_tuples_per_page(catalog);
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

uint8_t dbms_catalog_num_used(const system_catalog_t* catalog) {
  if (!catalog) {
    return 0;
  }

  catalog_record_t* last_record = &catalog->records[catalog->record_count - 1];
  return last_record->attribute_type == ATTRIBUTE_TYPE_UNUSED ? catalog->record_count - 1 : catalog->record_count;
}

uint64_t dbms_catalog_tuples_per_page(const system_catalog_t* catalog) {
  if (!catalog || catalog->tuple_size == 0) {
    return 0;
  }
  return DATA_SIZE / catalog->tuple_size;
}
