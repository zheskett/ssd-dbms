#include <string.h>

#include "dbms.h"
#include "executor/executor.h"
#include "executor/filter.h"
#include "executor/project.h"
#include "executor/seq_scan.h"
#include "query.h"
#include "ssdio.h"
#include "unity.h"

#define TEST_CATALOG_SIZE 6
#define TEST_TUPLE_SIZE 96

#define DB_PATH "test_executor.dat"

catalog_record_t test_catalog_records[TEST_CATALOG_SIZE] = {0};
system_catalog_t test_system_catalog = {0};
dbms_session_t* test_dbms_session = NULL;
dbms_manager_t* test_dbms_manager = NULL;

void setUp() {
  // Create a system catalog for testing
  catalog_record_t test_catalog_records_temp[] = {
      {"id", 4, ATTRIBUTE_TYPE_INT, 0},
      {"name", 50, ATTRIBUTE_TYPE_STRING, 1},
      {"salary", 4, ATTRIBUTE_TYPE_FLOAT, 2},
      {"department", 30, ATTRIBUTE_TYPE_STRING, 3},
      {"is_active", 1, ATTRIBUTE_TYPE_BOOL, 4},
      {PADDING_NAME, 6, ATTRIBUTE_TYPE_UNUSED, 5}};

  memcpy(test_catalog_records, test_catalog_records_temp, sizeof(test_catalog_records_temp));
  uint16_t tuple_size = NULL_BYTE_SIZE;
  for (size_t i = 0; i < sizeof(test_catalog_records_temp) / sizeof(catalog_record_t); i++) {
    tuple_size += test_catalog_records[i].attribute_size;
  }

  test_system_catalog.records = test_catalog_records;
  test_system_catalog.tuple_size = tuple_size;
  test_system_catalog.record_count = sizeof(test_catalog_records_temp) / sizeof(catalog_record_t);

  dbms_create_table(DB_PATH, &test_system_catalog);
  test_dbms_manager = dbms_init_dbms_manager();
  test_dbms_session = dbms_init_dbms_session(DB_PATH);
  dbms_add_session(test_dbms_manager, test_dbms_session);
}

void tearDown() {
  dbms_free_dbms_manager(test_dbms_manager);
  remove(DB_PATH);
}

// Helper to insert test tuples
static void insert_test_tuples(int count) {
  for (int i = 0; i < count; i++) {
    attribute_value_t attrs[TEST_CATALOG_SIZE - 1] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = i + 1},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "TestName"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 50000.0f + (float)(i * 1000)},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = (i % 2 == 0)}};
    dbms_insert_tuple(test_dbms_session, attrs);
  }
  dbms_flush_buffer_pool(test_dbms_session);
}

static void test_seq_scan_empty_table() {
  // Test scanning an empty table
  Operator* scan = seq_scan_create(test_dbms_session);
  TEST_ASSERT_NOT_NULL(scan);

  OP_OPEN(scan);
  tuple_t* tuple = OP_NEXT(scan);
  TEST_ASSERT_NULL(tuple);  // Empty table should return NULL immediately

  OP_CLOSE(scan);
  operator_free(scan);
}

static void test_seq_scan_single_tuple() {
  // Insert one tuple
  insert_test_tuples(1);

  Operator* scan = seq_scan_create(test_dbms_session);
  TEST_ASSERT_NOT_NULL(scan);

  OP_OPEN(scan);

  tuple_t* tuple = OP_NEXT(scan);
  TEST_ASSERT_NOT_NULL(tuple);
  TEST_ASSERT_EQUAL_INT(1, tuple->attributes[0].int_value);
  TEST_ASSERT_EQUAL_STRING("TestName", tuple->attributes[1].string_value);

  tuple = OP_NEXT(scan);
  TEST_ASSERT_NULL(tuple);  // No more tuples

  OP_CLOSE(scan);
  operator_free(scan);
}

static void test_seq_scan_multiple_tuples() {
  // Insert 10 tuples
  insert_test_tuples(10);

  Operator* scan = seq_scan_create(test_dbms_session);
  TEST_ASSERT_NOT_NULL(scan);

  OP_OPEN(scan);

  int count = 0;
  tuple_t* tuple;
  while ((tuple = OP_NEXT(scan)) != NULL) {
    count++;
    TEST_ASSERT_EQUAL_INT(count, tuple->attributes[0].int_value);
  }
  TEST_ASSERT_EQUAL_INT(10, count);

  OP_CLOSE(scan);
  operator_free(scan);
}

static void test_filter_with_predicate() {
  // Insert 10 tuples with ids 1-10
  insert_test_tuples(10);

  // Create filter: id > 5
  proposition_t prop = {
      .attribute_index = 0,
      .operator= OPERATOR_GREATER_THAN,
      .value = {.type = ATTRIBUTE_TYPE_INT, .int_value = 5}};
  selection_criteria_t criteria = {.propositions = &prop, .proposition_count = 1};

  Operator* scan = seq_scan_create(test_dbms_session);
  Operator* filter = filter_create(scan, test_dbms_session, &criteria);
  TEST_ASSERT_NOT_NULL(filter);

  OP_OPEN(filter);

  int count = 0;
  tuple_t* tuple;
  while ((tuple = OP_NEXT(filter)) != NULL) {
    count++;
    TEST_ASSERT_TRUE(tuple->attributes[0].int_value > 5);
  }
  TEST_ASSERT_EQUAL_INT(5, count);  // Should get tuples with id 6,7,8,9,10

  OP_CLOSE(filter);
  operator_free(filter);
}

static void test_project_columns() {
  // Insert some tuples
  insert_test_tuples(5);

  // Project only columns 0 (id) and 2 (salary)
  uint8_t columns[] = {0, 2};

  Operator* scan = seq_scan_create(test_dbms_session);
  Operator* project = project_create(scan, test_dbms_session, columns, 2);
  TEST_ASSERT_NOT_NULL(project);

  OP_OPEN(project);

  tuple_t* tuple = OP_NEXT(project);
  TEST_ASSERT_NOT_NULL(tuple);

  // Check projected values
  TEST_ASSERT_EQUAL_INT(1, tuple->attributes[0].int_value);       // id
  TEST_ASSERT_EQUAL_FLOAT(50000.0f, tuple->attributes[1].float_value);  // salary

  OP_CLOSE(project);
  operator_free(project);
}

static void test_left_deep_tree() {
  // Insert 10 tuples
  insert_test_tuples(10);

  // Build left-deep tree: Project -> Filter -> SeqScan
  // Filter: id > 3 AND id <= 7
  proposition_t props[2] = {
      {.attribute_index = 0,
       .operator= OPERATOR_GREATER_THAN,
       .value = {.type = ATTRIBUTE_TYPE_INT, .int_value = 3}},
      {.attribute_index = 0,
       .operator= OPERATOR_LESS_EQUAL,
       .value = {.type = ATTRIBUTE_TYPE_INT, .int_value = 7}}};
  selection_criteria_t criteria = {.propositions = props, .proposition_count = 2};

  // Project only id and name (columns 0 and 1)
  uint8_t columns[] = {0, 1};

  Operator* scan = seq_scan_create(test_dbms_session);
  Operator* filter = filter_create(scan, test_dbms_session, &criteria);
  Operator* project = project_create(filter, test_dbms_session, columns, 2);
  TEST_ASSERT_NOT_NULL(project);

  OP_OPEN(project);

  int count = 0;
  int expected_ids[] = {4, 5, 6, 7};
  tuple_t* tuple;
  while ((tuple = OP_NEXT(project)) != NULL) {
    TEST_ASSERT_EQUAL_INT(expected_ids[count], tuple->attributes[0].int_value);
    count++;
  }
  TEST_ASSERT_EQUAL_INT(4, count);  // Should get tuples 4, 5, 6, 7

  OP_CLOSE(project);
  operator_free(project);
}

static void test_pin_count_after_close() {
  // Insert some tuples
  insert_test_tuples(5);

  Operator* scan = seq_scan_create(test_dbms_session);
  TEST_ASSERT_NOT_NULL(scan);

  OP_OPEN(scan);

  // Consume some tuples
  OP_NEXT(scan);
  OP_NEXT(scan);

  OP_CLOSE(scan);
  operator_free(scan);

  // Verify all pages have pin_count = 0 after close
  for (uint32_t i = 0; i < BUFFER_POOL_SIZE; i++) {
    TEST_ASSERT_EQUAL_UINT32(0, test_dbms_session->buffer_pool->buffer_pages[i].pin_count);
  }
}

static void test_pinned_page_not_evicted() {
  // Create 5 pages on disk
  page_t temp_page;
  memset(&temp_page, 0, sizeof(page_t));
  for (uint64_t i = 1; i <= 5; i++) {
    dbms_init_page(test_dbms_session->catalog, &temp_page, i);
    ssdio_write_page(test_dbms_session->fd, i, &temp_page);
  }
  test_dbms_session->page_count = 5;
  ssdio_flush(test_dbms_session->fd);

  // Pin page 1
  buffer_page_t* p1 = dbms_pin_page(test_dbms_session, 1);
  TEST_ASSERT_NOT_NULL(p1);
  TEST_ASSERT_EQUAL_UINT32(1, p1->pin_count);

  // Load pages 2, 3, 4 to fill the buffer pool
  dbms_get_buffer_page(test_dbms_session, 2);
  dbms_get_buffer_page(test_dbms_session, 3);
  dbms_get_buffer_page(test_dbms_session, 4);

  // Now load page 5 - this should trigger eviction
  // Page 1 should NOT be evicted because it's pinned
  buffer_page_t* p5 = dbms_get_buffer_page(test_dbms_session, 5);
  TEST_ASSERT_NOT_NULL(p5);

  // Verify page 1 is still in buffer pool
  uint64_t index_out;
  TEST_ASSERT_TRUE(hash_table_get(test_dbms_session->buffer_pool->page_table, 1, &index_out));
  TEST_ASSERT_EQUAL_UINT32(1, p1->pin_count);

  // Unpin page 1 for cleanup
  dbms_unpin_page(test_dbms_session, p1);
  TEST_ASSERT_EQUAL_UINT32(0, p1->pin_count);
}

int main() {
  UNITY_BEGIN();
  RUN_TEST(test_seq_scan_empty_table);
  RUN_TEST(test_seq_scan_single_tuple);
  RUN_TEST(test_seq_scan_multiple_tuples);
  RUN_TEST(test_filter_with_predicate);
  RUN_TEST(test_project_columns);
  RUN_TEST(test_left_deep_tree);
  RUN_TEST(test_pin_count_after_close);
  RUN_TEST(test_pinned_page_not_evicted);

  return UNITY_END();
}

