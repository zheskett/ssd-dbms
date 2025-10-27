#include "dbms.h"
#include "unity.h"

void setUp() {
}
void tearDown() {
}

static void test_page_size() {
  TEST_ASSERT_EQUAL_INT(sizeof(page_t), PAGE_SIZE);
}

static void test_catalog_record_size() {
  TEST_ASSERT_EQUAL_INT(sizeof(catalog_record_t), CATALOG_RECORD_SIZE);
}

int main() {
  UNITY_BEGIN();
  RUN_TEST(test_page_size);
  RUN_TEST(test_catalog_record_size);

  return UNITY_END();
}