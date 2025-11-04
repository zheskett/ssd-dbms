#include "data_structures.h"
#include "unity.h"

hash_table_t* test_table = NULL;

void setUp(void) {
  test_table = hash_table_init(30);
}
void tearDown(void) {
  hash_table_free(test_table);
}

static void test_hash_table_init(void) {
  TEST_ASSERT_NOT_NULL(test_table);
  TEST_ASSERT_EQUAL_UINT64(32, test_table->bucket_count);  // Next power of two
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

int main() {
  UNITY_BEGIN();

  RUN_TEST(test_hash_table_init);
  RUN_TEST(test_hash_table_insert_and_get);
  RUN_TEST(test_hash_table_delete);

  return UNITY_END();
}