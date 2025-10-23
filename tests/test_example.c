#include "db.h"
#include "unity.h"

void setUp() {
}
void tearDown() {
}

static void test_add_basic() {
  TEST_ASSERT_EQUAL_INT(4, 2 + 2);
}

int main() {
  UNITY_BEGIN();
  RUN_TEST(test_add_basic);

  return UNITY_END();
}
