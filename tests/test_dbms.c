#include <string.h>

#include "dbms.h"
#include "unity.h"

#define TEST_CATALOG_SIZE 6
#define TEST_TUPLE_SIZE 96

#define DB_PATH "test_dbms.dat"

catalog_record_t test_catalog_records[TEST_CATALOG_SIZE] = {0};
system_catalog_t test_system_catalog = {0};
dbms_session_t* test_dbms_session = NULL;

void setUp() {
  // Create a system catalog for testing
  catalog_record_t test_catalog_records_temp[] = {
      {"id", 4, ATTRIBUTE_TYPE_INT, 0},         {"name", 50, ATTRIBUTE_TYPE_STRING, 1},
      {"salary", 4, ATTRIBUTE_TYPE_FLOAT, 2},   {"department", 30, ATTRIBUTE_TYPE_STRING, 3},
      {"is_active", 1, ATTRIBUTE_TYPE_BOOL, 4}, {PADDING_NAME, 6, ATTRIBUTE_TYPE_UNUSED, 5}};

  memcpy(test_catalog_records, test_catalog_records_temp, sizeof(test_catalog_records_temp));
  uint16_t tuple_size = NULL_BYTE_SIZE;
  for (size_t i = 0; i < sizeof(test_catalog_records_temp) / sizeof(catalog_record_t); i++) {
    tuple_size += test_catalog_records[i].attribute_size;
  }

  test_system_catalog.records = test_catalog_records;
  test_system_catalog.tuple_size = tuple_size;
  test_system_catalog.record_count = sizeof(test_catalog_records_temp) / sizeof(catalog_record_t);

  dbms_create_db(DB_PATH, &test_system_catalog);
  test_dbms_session = dbms_init_dbms_session(DB_PATH);
}
void tearDown() {
  dbms_free_dbms_session(test_dbms_session);
  remove(DB_PATH);
}

static void test_page_size() {
  TEST_ASSERT_EQUAL_INT(PAGE_SIZE, sizeof(page_t));
}

static void test_catalog_record_size() {
  TEST_ASSERT_EQUAL_INT(CATALOG_RECORD_SIZE, sizeof(catalog_record_t));
}

static void test_catalog_valid() {
  TEST_ASSERT_EQUAL_UINT8(TEST_CATALOG_SIZE, test_system_catalog.record_count);
  TEST_ASSERT_EQUAL_UINT16(TEST_TUPLE_SIZE, test_system_catalog.tuple_size);

  catalog_record_t* record = dbms_get_catalog_record(&test_system_catalog, 2);
  TEST_ASSERT_NOT_NULL(record);
  TEST_ASSERT_EQUAL_STRING("salary", record->attribute_name);
  TEST_ASSERT_EQUAL_UINT8(4, record->attribute_size);
  TEST_ASSERT_EQUAL_UINT8(ATTRIBUTE_TYPE_FLOAT, record->attribute_type);
  TEST_ASSERT_EQUAL_UINT8(2, record->attribute_order);
}

static void test_dbms_session_init() {
  TEST_ASSERT_NOT_NULL(test_dbms_session);
  TEST_ASSERT_TRUE(test_dbms_session->fd != -1);
  TEST_ASSERT_EQUAL_STRING("test_dbms", test_dbms_session->table_name);
  TEST_ASSERT_EQUAL_UINT8(TEST_CATALOG_SIZE, test_dbms_session->catalog->record_count);
  TEST_ASSERT_EQUAL_UINT16(TEST_TUPLE_SIZE, test_dbms_session->catalog->tuple_size);
}

static void test_dbms_catalog_num_used() {
  uint8_t used_attributes = dbms_catalog_num_used(&test_system_catalog);
  // May need to change if becomes multiple of 8
  TEST_ASSERT_EQUAL_INT(TEST_CATALOG_SIZE - 1, used_attributes);
}

static void test_dbms_insert_tuple() {
  attribute_value_t insert_attributes[TEST_CATALOG_SIZE - 1] = {
      {.type = ATTRIBUTE_TYPE_INT, .int_value = 1},
      {.type = ATTRIBUTE_TYPE_STRING, .string_value = "John Doe"},
      {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 55000.0f},
      {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
      {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};

  tuple_t* insert_tuple = dbms_insert_tuple(test_dbms_session, insert_attributes);
  TEST_ASSERT_NOT_NULL(insert_tuple);
  TEST_ASSERT_FALSE(insert_tuple->is_null);
  TEST_ASSERT_EQUAL_UINT64(0, insert_tuple->id.slot_id);
  TEST_ASSERT_EQUAL_UINT64(1, insert_tuple->id.page_id);
  TEST_ASSERT_EQUAL_INT(1, insert_tuple->attributes[0].int_value);
  TEST_ASSERT_EQUAL_STRING("John Doe", insert_tuple->attributes[1].string_value);
  TEST_ASSERT_EQUAL_FLOAT(55000.0f, insert_tuple->attributes[2].float_value);
  TEST_ASSERT_EQUAL_STRING("Engineering", insert_tuple->attributes[3].string_value);
  TEST_ASSERT_TRUE(insert_tuple->attributes[4].bool_value);

  buffer_page_t* fetched_buffer_page = dbms_get_buffer_page(test_dbms_session, 1);
  TEST_ASSERT_NOT_NULL(fetched_buffer_page);
  TEST_ASSERT_EQUAL_UINT64(1, fetched_buffer_page->page_id);
  TEST_ASSERT_TRUE(fetched_buffer_page->is_dirty);
  TEST_ASSERT_EQUAL_UINT64(0, fetched_buffer_page->tuples[0].id.slot_id);
  TEST_ASSERT_EQUAL_UINT64(1, fetched_buffer_page->tuples[0].id.page_id);
  TEST_ASSERT_EQUAL_INT(1, fetched_buffer_page->tuples[0].attributes[0].int_value);
  TEST_ASSERT_EQUAL_STRING("John Doe", fetched_buffer_page->tuples[0].attributes[1].string_value);
  TEST_ASSERT_EQUAL_FLOAT(55000.0f, fetched_buffer_page->tuples[0].attributes[2].float_value);
  TEST_ASSERT_EQUAL_STRING("Engineering", fetched_buffer_page->tuples[0].attributes[3].string_value);
  TEST_ASSERT_TRUE(fetched_buffer_page->tuples[0].attributes[4].bool_value);

  dbms_flush_buffer_pool(test_dbms_session);
  TEST_ASSERT_TRUE(test_dbms_session->buffer_pool->buffer_pages[0].is_free);

  // Assert database file exists
  FILE* db_file = fopen(DB_PATH, "r");
  TEST_ASSERT_TRUE(db_file != NULL);
  fclose(db_file);

  buffer_page_t* reloaded_buffer_page = dbms_get_buffer_page(test_dbms_session, 1);
  TEST_ASSERT_NOT_NULL(reloaded_buffer_page);
  TEST_ASSERT_EQUAL_UINT64(1, reloaded_buffer_page->page_id);
  TEST_ASSERT_EQUAL_INT(false, reloaded_buffer_page->is_dirty);
  TEST_ASSERT_EQUAL_UINT64(0, reloaded_buffer_page->tuples[0].id.slot_id);
  TEST_ASSERT_EQUAL_UINT64(1, reloaded_buffer_page->tuples[0].id.page_id);
  TEST_ASSERT_EQUAL_INT(1, reloaded_buffer_page->tuples[0].attributes[0].int_value);
  TEST_ASSERT_EQUAL_STRING("John Doe", reloaded_buffer_page->tuples[0].attributes[1].string_value);
  TEST_ASSERT_EQUAL_FLOAT(55000.0f, reloaded_buffer_page->tuples[0].attributes[2].float_value);
  TEST_ASSERT_EQUAL_STRING("Engineering", reloaded_buffer_page->tuples[0].attributes[3].string_value);
  TEST_ASSERT_TRUE(reloaded_buffer_page->tuples[0].attributes[4].bool_value);
}

static void test_dbms_fill_page() {
  attribute_value_t insert_attributes[TEST_CATALOG_SIZE - 1] = {
      {.type = ATTRIBUTE_TYPE_INT, .int_value = 1},
      {.type = ATTRIBUTE_TYPE_STRING, .string_value = "John Doe"},
      {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 55000.0f},
      {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
      {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};

  size_t tuples_per_page = dbms_catalog_tuples_per_page(&test_system_catalog);
  for (size_t i = 0; i < tuples_per_page; i++) {
    tuple_t* insert_tuple = dbms_insert_tuple(test_dbms_session, insert_attributes);
    TEST_ASSERT_NOT_NULL(insert_tuple);
    TEST_ASSERT_EQUAL_UINT64(1, insert_tuple->id.page_id);
    if (i < tuples_per_page - 1) {
      TEST_ASSERT_EQUAL_UINT64((i + 1) * (uint64_t)test_dbms_session->catalog->tuple_size,
                               test_dbms_session->buffer_pool->buffer_pages[0].page->free_space_head);
    } else {
      TEST_ASSERT_EQUAL_UINT64(PAGE_SIZE, test_dbms_session->buffer_pool->buffer_pages[0].page->free_space_head);
    }
  }
  TEST_ASSERT_EQUAL_UINT64(tuples_per_page, test_dbms_session->buffer_pool->buffer_pages[0].page->tuples_per_page);
  TEST_ASSERT_EQUAL_UINT32(1, test_dbms_session->buffer_pool->page_count);
  TEST_ASSERT_EQUAL_UINT32(1, test_dbms_session->page_count);
}

int main() {
  UNITY_BEGIN();
  RUN_TEST(test_page_size);
  RUN_TEST(test_catalog_record_size);
  RUN_TEST(test_catalog_valid);
  RUN_TEST(test_dbms_session_init);
  RUN_TEST(test_dbms_catalog_num_used);
  RUN_TEST(test_dbms_insert_tuple);
  RUN_TEST(test_dbms_fill_page);

  return UNITY_END();
}