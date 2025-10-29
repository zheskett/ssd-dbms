#include "dbms.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "align.h"
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
  memset(first_page, 0, sizeof(page_t));
  if (!dbms_init_page(catalog, first_page)) {
    fprintf(stderr, "Failed to initialize first page\n");
    free(first_page);
    ssdio_close(fd);
    return false;
  }

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

  session->fd = -1;
  session->update_ctr = 0;

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

  off_t file_size = ssdio_get_file_size(session->fd);
  if (file_size < PAGE_SIZE || file_size % PAGE_SIZE != 0) {
    fprintf(stderr, "Invalid database file size: %lld bytes\n", file_size);
    dbms_free_dbms_session(session);
    return NULL;
  }
  session->page_count = (uint32_t)(file_size / PAGE_SIZE) - 1;  // Exclude catalog page

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
  memset(pages, 0, BUFFER_POOL_SIZE * sizeof(page_t));
  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    session->buffer_pool->buffer_pages[i].is_free = true;
    session->buffer_pool->buffer_pages[i].is_dirty = false;
    session->buffer_pool->buffer_pages[i].last_updated = 0;
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
  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);
  attribute_value_t* all_attribute_values =
      calloc(BUFFER_POOL_SIZE * tuples_per_page * num_attributes, sizeof(attribute_value_t));
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
      tuple->is_null = true;
      tuple->attributes = &all_attribute_values[(i * tuples_per_page + j) * num_attributes];

      // For each attribute, correctly set type based on catalog
      for (uint8_t k = 0; k < num_attributes; k++) {
        catalog_record_t* record = dbms_get_catalog_record(session->catalog, k);
        if (record) {
          tuple->attributes[k].type = record->attribute_type;
        }
      }
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

buffer_page_t* dbms_get_buffer_page(dbms_session_t* session, uint64_t page_id) {
  if (!session || !session->buffer_pool) {
    return NULL;
  }

  // Check if page is already in buffer pool
  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    buffer_page_t* buffer_page = &session->buffer_pool->buffer_pages[i];
    if (!buffer_page->is_free && buffer_page->page_id == page_id) {
      return buffer_page;
    }
  }

  // Page not found in buffer pool, get a free page or evict one
  buffer_page_t* target_page = dbms_run_buffer_pool_policy(session);
  if (!target_page) {
    fprintf(stderr, "Failed to find or evict a buffer page for page ID %llu\n", page_id);
    return NULL;
  }

  // Load the requested page from disk
  if (!ssdio_read_page(session->fd, page_id, target_page->page)) {
    fprintf(stderr, "Failed to read page %llu from disk\n", page_id);
    return NULL;
  }

  target_page->is_free = false;
  target_page->is_dirty = false;
  target_page->page_id = page_id;
  target_page->last_updated = session->update_ctr++;
  session->buffer_pool->page_count++;

  // Set all the tuples and attribute values to match the new page
  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);
  for (uint64_t j = 0; j < tuples_per_page; j++) {
    tuple_t* tuple = &target_page->tuples[j];
    tuple->id.page_id = page_id;
    tuple->id.slot_id = j;

    // Check if tuple is null based on null byte
    char* tuple_data = target_page->page->data + (j * session->catalog->tuple_size);
    tuple->is_null = (tuple_data[0] == 0);

    for (uint8_t k = 0; k < num_attributes; k++) {
      catalog_record_t* record = dbms_get_catalog_record(session->catalog, k);
      if (record) {
        tuple->attributes[k].type = record->attribute_type;

        // Set values
        // Even if the tuple is null, we set the attribute values for easier access later
        off_t offset = dbms_get_attribute_offset(session->catalog, k);
        char* attribute_data = tuple_data + offset;
        switch (record->attribute_type) {
          case ATTRIBUTE_TYPE_INT:
            tuple->attributes[k].int_value = (int32_t)load_u32(attribute_data);
            break;
          case ATTRIBUTE_TYPE_FLOAT:
            tuple->attributes[k].float_value = load_f32(attribute_data);
            break;
          case ATTRIBUTE_TYPE_STRING:
            tuple->attributes[k].string_value = attribute_data;
            break;
          case ATTRIBUTE_TYPE_BOOL:
            tuple->attributes[k].bool_value = (load_u8(attribute_data) != 0);
            break;
          default:
            break;
        }
      }
    }
  }

  return target_page;
}

buffer_page_t* dbms_run_buffer_pool_policy(dbms_session_t* session) {
  if (!session || !session->buffer_pool) {
    return NULL;
  }

  // Still free space in buffer pool
  if (session->buffer_pool->page_count < BUFFER_POOL_SIZE) {
    // There is still free space in the buffer pool
    for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
      buffer_page_t* buffer_page = &session->buffer_pool->buffer_pages[i];
      if (buffer_page->is_free) {
        return buffer_page;
      }
    }
  }

  // TODO: Eviction policy
  return NULL;
}

void dbms_flush_buffer_page(dbms_session_t* session, buffer_page_t* buffer_page) {
  if (!session || !buffer_page) {
    return;
  }

  if (buffer_page->is_dirty && !buffer_page->is_free) {
    if (!ssdio_write_page(session->fd, buffer_page->page_id, buffer_page->page)) {
      fprintf(stderr, "Failed to flush buffer page %llu to disk\n", buffer_page->page_id);
      return;
    }

    // TODO: Better flushing
    ssdio_flush(session->fd);
  }

  if (!buffer_page->is_free) {
    session->buffer_pool->page_count--;
  }

  buffer_page->last_updated = 0;
  buffer_page->is_free = true;
  buffer_page->page_id = 0;
  buffer_page->is_dirty = false;
}

void dbms_flush_buffer_pool(dbms_session_t* session) {
  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    buffer_page_t* buffer_page = &session->buffer_pool->buffer_pages[i];
    dbms_flush_buffer_page(session, buffer_page);
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

  page->free_space_head = 0;
  page->tuples_per_page = dbms_catalog_tuples_per_page(catalog);
  if (page->tuples_per_page == 0) {
    fprintf(stderr, "Tuple size %u too large to fit in page\n", catalog->tuple_size);
    return false;
  }
  if (catalog->tuple_size % 8 != 0 || catalog->tuple_size < 16) {
    fprintf(stderr, "Tuple size %u is invalid\n", catalog->tuple_size);
    return false;
  }

  // For each tuple, add to free space linked list
  for (uint64_t i = 0; i < page->tuples_per_page; i++) {
    uint64_t tuple_offset = i * catalog->tuple_size + FREE_POINTER_OFFSET;
    uint64_t null_byte_offset = tuple_offset - FREE_POINTER_OFFSET;
    uint64_t* next_free_ptr = (uint64_t*)&page->data[tuple_offset];
    uint64_t* null_byte_ptr = (uint64_t*)&page->data[null_byte_offset];

    *null_byte_ptr = 0;  // Mark as free
    if (i == page->tuples_per_page - 1) {
      *next_free_ptr = PAGE_SIZE;  // End of free list
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
  return (uint64_t)DATA_SIZE / catalog->tuple_size;
}

buffer_page_t* dbms_find_page_with_free_space(dbms_session_t* session) {
  if (!session || !session->buffer_pool) {
    return NULL;
  }

  // Check if any pages in buffer pool have free space
  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    buffer_page_t* buffer_page = &session->buffer_pool->buffer_pages[i];
    if (!buffer_page->is_free) {
      page_t* page = buffer_page->page;
      // There is free space if free space head is not at end of data
      if (page->free_space_head != PAGE_SIZE) {
        return buffer_page;
      }
    }
  }

  // No pages in buffer pool have free space, need to load a new page
  // TODO: Load actual pages with free space from disk
  buffer_page_t* target_page = dbms_get_buffer_page(session, 1);
  if (!target_page) {
    fprintf(stderr, "Failed to load page with free space from disk\n");
    return NULL;
  }
  page_t* page = target_page->page;
  if (page->free_space_head == 0) {
    fprintf(stderr, "Loaded page has no free space\n");
    return NULL;
  }

  return target_page;
}

tuple_t* dbms_insert_tuple(dbms_session_t* session, attribute_value_t* attributes) {
  if (!session || !attributes) {
    return NULL;
  }

  // Find a page with free space
  buffer_page_t* target_page = dbms_find_page_with_free_space(session);
  if (!target_page) {
    fprintf(stderr, "Failed to find a page with free space for inserting tuple\n");
    return NULL;
  }

  // Insert tuple into the target page
  page_t* page = target_page->page;
  uint64_t free_space_offset = page->free_space_head;

  // free_space_head = PAGE_SIZE means no free space
  if (free_space_offset >= PAGE_SIZE) {
    fprintf(stderr, "No free space available in the target page\n");
    return NULL;
  }

  // Update free space head to next free tuple
  uint64_t next_free_ptr = *(uint64_t*)&page->data[free_space_offset + FREE_POINTER_OFFSET];
  page->free_space_head = next_free_ptr;

  // Write attribute values into the page and into the tuples
  uint64_t slot_id = free_space_offset / session->catalog->tuple_size;
  tuple_t* tuple = &target_page->tuples[slot_id];
  char* tuple_page_loc = &page->data[free_space_offset];
  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);

  tuple_page_loc[0] = 1;  // Mark as not null
  // Zero out the rest of the tuple data
  memset(tuple_page_loc + NULL_BYTE_SIZE, 0, session->catalog->tuple_size - NULL_BYTE_SIZE);
  tuple->is_null = false;
  off_t offset = NULL_BYTE_SIZE;
  for (uint8_t i = 0; i < num_attributes; i++) {
    catalog_record_t* record = dbms_get_catalog_record(session->catalog, i);
    char* page_attribute_ptr = tuple_page_loc + offset;
    attribute_value_t* tuple_attr = &tuple->attributes[i];
    switch (record->attribute_type) {
      case ATTRIBUTE_TYPE_INT:
        tuple_attr->int_value = attributes[i].int_value;
        store_u32(page_attribute_ptr, (uint32_t)attributes[i].int_value);
        break;
      case ATTRIBUTE_TYPE_FLOAT:
        tuple_attr->float_value = attributes[i].float_value;
        store_f32(page_attribute_ptr, attributes[i].float_value);
        break;
      case ATTRIBUTE_TYPE_STRING:
        // This should have size effect of also affecting the tuple string value
        memcpy(page_attribute_ptr, attributes[i].string_value,
               strnlen(attributes[i].string_value, record->attribute_size));
        break;
      case ATTRIBUTE_TYPE_BOOL:
        tuple_attr->bool_value = attributes[i].bool_value;
        store_u8(page_attribute_ptr, attributes[i].bool_value ? 1 : 0);
        break;
      default:
        break;
    }
    offset += record->attribute_size;
  }

  target_page->is_dirty = true;
  target_page->last_updated = session->update_ctr++;

  return tuple;
}

bool dbms_delete_tuple(dbms_session_t* session, tuple_id_t tuple_id) {
  if (!session) {
    return false;
  }

  buffer_page_t* buffer_page = dbms_get_buffer_page(session, tuple_id.page_id);
  if (!buffer_page) {
    return false;
  }

  page_t* page = buffer_page->page;
  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  if (tuple_id.slot_id >= tuples_per_page) {
    fprintf(stderr, "Invalid slot ID %llu for page ID %llu\n", tuple_id.slot_id, tuple_id.page_id);
    return false;
  }

  // Check if tuple is already null
  tuple_t* tuple = &buffer_page->tuples[tuple_id.slot_id];
  if (tuple->is_null) {
    fprintf(stderr, "Tuple %llu:%llu is already null\n", tuple_id.page_id, tuple_id.slot_id);
    return false;
  }

  // Get tuple data location
  uint64_t tuple_offset = tuple_id.slot_id * session->catalog->tuple_size;
  char* tuple_data = &page->data[tuple_offset];
  // Nullify the tuple data
  memset(tuple_data, 0, session->catalog->tuple_size);

  // Add tuple back to free space linked list
  uint64_t* next_free_ptr = (uint64_t*)&tuple_data[FREE_POINTER_OFFSET];
  *next_free_ptr = page->free_space_head;
  page->free_space_head = tuple_offset;

  // Mark tuple as null in buffer page
  tuple->is_null = true;

  buffer_page->is_dirty = true;
  buffer_page->last_updated = session->update_ctr++;

  return true;
}

tuple_t* dbms_get_tuple(dbms_session_t* session, tuple_id_t tuple_id) {
  if (!session) {
    return NULL;
  }

  uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  if (tuple_id.slot_id >= tuples_per_page) {
    fprintf(stderr, "Invalid slot ID %llu for page ID %llu\n", tuple_id.slot_id, tuple_id.page_id);
    return NULL;
  }

  buffer_page_t* buffer_page = dbms_get_buffer_page(session, tuple_id.page_id);
  if (!buffer_page) {
    return NULL;
  }

  tuple_t* tuple = &buffer_page->tuples[tuple_id.slot_id];
  if (tuple->is_null) {
    return NULL;
  }

  return tuple;
}
