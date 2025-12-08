#include <stdlib.h>
#include <string.h>

#include "dbms.h"
#include "executor/executor.h"
#include "executor/filter.h"
#include "executor/nested_loop_join.h"
#include "executor/project.h"
#include "executor/seq_scan.h"
#include "query.h"
#include "ssdio.h"
#include "unity.h"

#define TEST_CATALOG_SIZE 6
#define TEST_TUPLE_SIZE 96

#define DB_PATH_A "test_advanced_a.dat"
#define DB_PATH_B "test_advanced_b.dat"

catalog_record_t test_catalog_records[TEST_CATALOG_SIZE] = {0};
system_catalog_t test_system_catalog = {0};
dbms_session_t* session_a = NULL;
dbms_session_t* session_b = NULL;
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

    // Create two tables for join tests
    dbms_create_table(DB_PATH_A, &test_system_catalog);
    dbms_create_table(DB_PATH_B, &test_system_catalog);

    test_dbms_manager = dbms_init_dbms_manager();
    session_a = dbms_init_dbms_session(DB_PATH_A);
    session_b = dbms_init_dbms_session(DB_PATH_B);
    dbms_add_session(test_dbms_manager, session_a);
    dbms_add_session(test_dbms_manager, session_b);
}

void tearDown() {
    dbms_free_dbms_manager(test_dbms_manager);
    remove(DB_PATH_A);
    remove(DB_PATH_B);
}

// Helper to insert test tuples into a session
static void insert_tuples(dbms_session_t* session, int count, int start_id) {
    for (int i = 0; i < count; i++) {
        attribute_value_t attrs[TEST_CATALOG_SIZE - 1] = {
            {.type = ATTRIBUTE_TYPE_INT, .int_value = start_id + i},
            {.type = ATTRIBUTE_TYPE_STRING, .string_value = "TestName"},
            {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 50000.0f + (float)(i * 1000)},
            {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
            {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = (i % 2 == 0)}};
        dbms_insert_tuple(session, attrs);
    }
    dbms_flush_buffer_pool(session);
}

// ============================================================================
// Reset functionality tests
// ============================================================================

static void test_seq_scan_reset() {
    insert_tuples(session_a, 5, 1);

    Operator* scan = seq_scan_create(session_a);
    TEST_ASSERT_NOT_NULL(scan);

    OP_OPEN(scan);

    // Read first two tuples
    tuple_t* t1 = OP_NEXT(scan);
    TEST_ASSERT_NOT_NULL(t1);
    TEST_ASSERT_EQUAL_INT(1, t1->attributes[0].int_value);

    tuple_t* t2 = OP_NEXT(scan);
    TEST_ASSERT_NOT_NULL(t2);
    TEST_ASSERT_EQUAL_INT(2, t2->attributes[0].int_value);

    // Reset and verify we get the first tuple again
    OP_RESET(scan);

    tuple_t* t_after_reset = OP_NEXT(scan);
    TEST_ASSERT_NOT_NULL(t_after_reset);
    TEST_ASSERT_EQUAL_INT(1, t_after_reset->attributes[0].int_value);

    OP_CLOSE(scan);
    operator_free(scan);
}

static void test_filter_reset() {
    insert_tuples(session_a, 10, 1);

    // Filter: id > 5
    proposition_t prop = {
        .attribute_index = 0,
        .operator= OPERATOR_GREATER_THAN,
        .value = {.type = ATTRIBUTE_TYPE_INT, .int_value = 5}};
    selection_criteria_t criteria = {.propositions = &prop, .proposition_count = 1};

    Operator* scan = seq_scan_create(session_a);
    Operator* filter = filter_create(scan, session_a, &criteria);

    OP_OPEN(filter);

    // Get first filtered tuple (should be id=6)
    tuple_t* t1 = OP_NEXT(filter);
    TEST_ASSERT_NOT_NULL(t1);
    TEST_ASSERT_EQUAL_INT(6, t1->attributes[0].int_value);

    // Get second (id=7)
    tuple_t* t2 = OP_NEXT(filter);
    TEST_ASSERT_NOT_NULL(t2);
    TEST_ASSERT_EQUAL_INT(7, t2->attributes[0].int_value);

    // Reset
    OP_RESET(filter);

    // Should get id=6 again
    tuple_t* t_after_reset = OP_NEXT(filter);
    TEST_ASSERT_NOT_NULL(t_after_reset);
    TEST_ASSERT_EQUAL_INT(6, t_after_reset->attributes[0].int_value);

    OP_CLOSE(filter);
    operator_free(filter);
}

// ============================================================================
// Cross-Product (Nested Loop Join) tests
// ============================================================================

static void test_cross_product_basic() {
    // Insert 3 tuples in table A
    insert_tuples(session_a, 3, 1);
    // Insert 2 tuples in table B
    insert_tuples(session_b, 2, 100);

    uint8_t num_attrs = dbms_catalog_num_used(session_a->catalog);

    Operator* scan_a = seq_scan_create(session_a);
    Operator* scan_b = seq_scan_create(session_b);
    Operator* join = nested_loop_join_create(scan_a, scan_b, session_a, num_attrs, num_attrs);
    TEST_ASSERT_NOT_NULL(join);

    OP_OPEN(join);

    int count = 0;
    tuple_t* tuple;
    while ((tuple = OP_NEXT(join)) != NULL) {
        count++;
        // First num_attrs columns are from outer, next num_attrs from inner
        int outer_id = tuple->attributes[0].int_value;
        int inner_id = tuple->attributes[num_attrs].int_value;
        
        // Outer ids should be 1, 2, 3
        TEST_ASSERT_TRUE(outer_id >= 1 && outer_id <= 3);
        // Inner ids should be 100, 101
        TEST_ASSERT_TRUE(inner_id >= 100 && inner_id <= 101);
    }

    // 3 * 2 = 6 combinations
    TEST_ASSERT_EQUAL_INT(6, count);

    OP_CLOSE(join);
    operator_free(join);
}

static void test_cross_product_empty_inner() {
    // Insert tuples only in table A
    insert_tuples(session_a, 3, 1);
    // Table B is empty

    uint8_t num_attrs = dbms_catalog_num_used(session_a->catalog);

    Operator* scan_a = seq_scan_create(session_a);
    Operator* scan_b = seq_scan_create(session_b);
    Operator* join = nested_loop_join_create(scan_a, scan_b, session_a, num_attrs, num_attrs);

    OP_OPEN(join);

    // Should get no results since inner is empty
    tuple_t* tuple = OP_NEXT(join);
    TEST_ASSERT_NULL(tuple);

    OP_CLOSE(join);
    operator_free(join);
}

static void test_cross_product_empty_outer() {
    // Table A is empty
    // Insert tuples in table B
    insert_tuples(session_b, 3, 100);

    uint8_t num_attrs = dbms_catalog_num_used(session_a->catalog);

    Operator* scan_a = seq_scan_create(session_a);
    Operator* scan_b = seq_scan_create(session_b);
    Operator* join = nested_loop_join_create(scan_a, scan_b, session_a, num_attrs, num_attrs);

    OP_OPEN(join);

    // Should get no results since outer is empty
    tuple_t* tuple = OP_NEXT(join);
    TEST_ASSERT_NULL(tuple);

    OP_CLOSE(join);
    operator_free(join);
}

// ============================================================================
// DISTINCT (duplicate elimination) tests
// ============================================================================

static void test_distinct_eliminates_duplicates() {
    // Insert duplicate tuples with same id
    // ids: 1, 2, 1, 3, 2 (duplicates of 1 and 2)
    attribute_value_t attrs1[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 1},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Alice"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 50000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};
    dbms_insert_tuple(session_a, attrs1);

    attribute_value_t attrs2[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 2},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Bob"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 60000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Sales"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = false}};
    dbms_insert_tuple(session_a, attrs2);

    attribute_value_t attrs3[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 1},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Alice"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 50000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};
    dbms_insert_tuple(session_a, attrs3);

    attribute_value_t attrs4[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 3},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Charlie"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 70000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "HR"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};
    dbms_insert_tuple(session_a, attrs4);

    attribute_value_t attrs5[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 2},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Bob"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 60000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Sales"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = false}};
    dbms_insert_tuple(session_a, attrs5);

    dbms_flush_buffer_pool(session_a);

    // Project all columns with DISTINCT
    uint8_t num_attrs = dbms_catalog_num_used(session_a->catalog);
    uint8_t* columns = calloc(num_attrs, sizeof(uint8_t));
    for (uint8_t i = 0; i < num_attrs; i++) {
        columns[i] = i;
    }

    Operator* scan = seq_scan_create(session_a);
    Operator* project = project_create(scan, session_a, columns, num_attrs, true);  // is_distinct = true

    OP_OPEN(project);

    int count = 0;
    tuple_t* tuple;
    while ((tuple = OP_NEXT(project)) != NULL) {
        count++;
    }

    // Should get 3 unique tuples (ids 1, 2, 3)
    TEST_ASSERT_EQUAL_INT(3, count);

    OP_CLOSE(project);
    operator_free(project);
    free(columns);
}

static void test_distinct_on_single_column() {
    // Insert tuples with different full data but same projected column
    attribute_value_t attrs1[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 1},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Alice"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 50000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Engineering"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};
    dbms_insert_tuple(session_a, attrs1);

    attribute_value_t attrs2[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 1},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Bob"},  // Different name
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 60000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Sales"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = false}};
    dbms_insert_tuple(session_a, attrs2);

    attribute_value_t attrs3[] = {
        {.type = ATTRIBUTE_TYPE_INT, .int_value = 2},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "Charlie"},
        {.type = ATTRIBUTE_TYPE_FLOAT, .float_value = 70000.0f},
        {.type = ATTRIBUTE_TYPE_STRING, .string_value = "HR"},
        {.type = ATTRIBUTE_TYPE_BOOL, .bool_value = true}};
    dbms_insert_tuple(session_a, attrs3);

    dbms_flush_buffer_pool(session_a);

    // Project only column 0 (id) with DISTINCT
    uint8_t columns[] = {0};

    Operator* scan = seq_scan_create(session_a);
    Operator* project = project_create(scan, session_a, columns, 1, true);

    OP_OPEN(project);

    int count = 0;
    tuple_t* tuple;
    while ((tuple = OP_NEXT(project)) != NULL) {
        count++;
    }

    // Should get 2 unique ids (1 and 2)
    TEST_ASSERT_EQUAL_INT(2, count);

    OP_CLOSE(project);
    operator_free(project);
}

static void test_distinct_with_filter() {
    // Insert tuples
    insert_tuples(session_a, 10, 1);
    // Insert duplicates of some
    insert_tuples(session_a, 3, 3);  // ids 3, 4, 5 (duplicates)

    dbms_flush_buffer_pool(session_a);

    // Filter: id > 2 AND id <= 6
    proposition_t props[2] = {
        {.attribute_index = 0,
         .operator= OPERATOR_GREATER_THAN,
         .value = {.type = ATTRIBUTE_TYPE_INT, .int_value = 2}},
        {.attribute_index = 0,
         .operator= OPERATOR_LESS_EQUAL,
         .value = {.type = ATTRIBUTE_TYPE_INT, .int_value = 6}}};
    selection_criteria_t criteria = {.propositions = props, .proposition_count = 2};

    // Project id column with DISTINCT
    uint8_t columns[] = {0};

    Operator* scan = seq_scan_create(session_a);
    Operator* filter = filter_create(scan, session_a, &criteria);
    Operator* project = project_create(filter, session_a, columns, 1, true);

    OP_OPEN(project);

    int count = 0;
    tuple_t* tuple;
    while ((tuple = OP_NEXT(project)) != NULL) {
        int id = tuple->attributes[0].int_value;
        TEST_ASSERT_TRUE(id >= 3 && id <= 6);
        count++;
    }

    // Should get 4 unique ids (3, 4, 5, 6) despite having duplicates
    TEST_ASSERT_EQUAL_INT(4, count);

    OP_CLOSE(project);
    operator_free(project);
}

static void test_project_reset_clears_distinct_set() {
    // Insert duplicates
    insert_tuples(session_a, 3, 1);
    insert_tuples(session_a, 3, 1);  // Duplicates

    dbms_flush_buffer_pool(session_a);

    uint8_t columns[] = {0};  // Just id
    Operator* scan = seq_scan_create(session_a);
    Operator* project = project_create(scan, session_a, columns, 1, true);

    OP_OPEN(project);

    // First pass - should get 3 unique
    int count1 = 0;
    while (OP_NEXT(project) != NULL) count1++;
    TEST_ASSERT_EQUAL_INT(3, count1);

    // Reset
    OP_RESET(project);

    // Second pass - should still get 3 unique (set was cleared)
    int count2 = 0;
    while (OP_NEXT(project) != NULL) count2++;
    TEST_ASSERT_EQUAL_INT(3, count2);

    OP_CLOSE(project);
    operator_free(project);
}

int main() {
    UNITY_BEGIN();

    // Reset tests
    RUN_TEST(test_seq_scan_reset);
    RUN_TEST(test_filter_reset);

    // Cross-product tests
    RUN_TEST(test_cross_product_basic);
    RUN_TEST(test_cross_product_empty_inner);
    RUN_TEST(test_cross_product_empty_outer);

    // DISTINCT tests
    RUN_TEST(test_distinct_eliminates_duplicates);
    RUN_TEST(test_distinct_on_single_column);
    RUN_TEST(test_distinct_with_filter);
    RUN_TEST(test_project_reset_clears_distinct_set);

    return UNITY_END();
}

