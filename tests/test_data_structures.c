#include "data_structures.h"
#include "unity.h"

hash_table_t* test_table = NULL;

void setUp(void) {
  test_table = hash_table_init(6);
}
void tearDown(void) {
  hash_table_free(test_table);
}

static void test_hash_table_init(void) {
  TEST_ASSERT_NOT_NULL(test_table);
  TEST_ASSERT_EQUAL_UINT64(8, test_table->bucket_count);  // Next power of two
}

static void test_hash_table_insert_and_get(void) {
  TEST_ASSERT_NOT_NULL(test_table);

  // Insert key-value pairs
  TEST_ASSERT_TRUE(hash_table_insert(test_table, 1, 100));
  TEST_ASSERT_TRUE(hash_table_insert(test_table, 2, 200));
  TEST_ASSERT_TRUE(hash_table_insert(test_table, 3, 300));

  // Retrieve values
  uint64_t value;
  TEST_ASSERT_TRUE(hash_table_get(test_table, 1, &value));
  TEST_ASSERT_EQUAL_UINT64(100, value);

  TEST_ASSERT_TRUE(hash_table_get(test_table, 2, &value));
  TEST_ASSERT_EQUAL_UINT64(200, value);

  TEST_ASSERT_TRUE(hash_table_get(test_table, 3, &value));
  TEST_ASSERT_EQUAL_UINT64(300, value);

  // Try to get a non-existent key
  TEST_ASSERT_FALSE(hash_table_get(test_table, 4, &value));
}

static void test_hash_table_delete(void) {
  TEST_ASSERT_NOT_NULL(test_table);

  // Insert key-value pairs
  TEST_ASSERT_TRUE(hash_table_insert(test_table, 1, 100));
  TEST_ASSERT_TRUE(hash_table_insert(test_table, 2, 200));

  // Delete a key
  TEST_ASSERT_TRUE(hash_table_delete(test_table, 1));

  // Try to get the deleted key
  uint64_t value;
  TEST_ASSERT_FALSE(hash_table_get(test_table, 1, &value));

  // Ensure other key is still retrievable
  TEST_ASSERT_TRUE(hash_table_get(test_table, 2, &value));
  TEST_ASSERT_EQUAL_UINT64(200, value);
}

static void test_hash_table_large_number_of_elements(void) {
  TEST_ASSERT_NOT_NULL(test_table);

  const int num_elements = 2000;
  // Insert a large number of elements
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_TRUE(hash_table_insert(test_table, i * 2, i * 10));
  }

  // Insert elements with odd keys
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_TRUE(hash_table_insert(test_table, i * 2 + 1, i * 10 + 5));
  }

  // Retrieve and verify all elements
  uint64_t value;
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_TRUE(hash_table_get(test_table, i * 2, &value));
    TEST_ASSERT_EQUAL_UINT64(i * 10, value);
  }
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_TRUE(hash_table_get(test_table, i * 2 + 1, &value));
    TEST_ASSERT_EQUAL_UINT64(i * 10 + 5, value);
  }

  // Delete all elements
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_TRUE(hash_table_delete(test_table, i * 2));
  }
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_TRUE(hash_table_delete(test_table, i * 2 + 1));
  }

  // Ensure all elements are deleted
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_FALSE(hash_table_get(test_table, i * 2, &value));
  }
  for (uint64_t i = 0; i < num_elements; i++) {
    TEST_ASSERT_FALSE(hash_table_get(test_table, i * 2 + 1, &value));
  }
}

int main() {
  UNITY_BEGIN();

  RUN_TEST(test_hash_table_init);
  RUN_TEST(test_hash_table_insert_and_get);
  RUN_TEST(test_hash_table_delete);
  RUN_TEST(test_hash_table_large_number_of_elements);

  return UNITY_END();
}