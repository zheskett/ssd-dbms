#ifndef QUERY_H
#define QUERY_H

#include "dbms.h"

#define OPERATOR_EQUAL 1
#define OPERATOR_NOT_EQUAL 2
#define OPERATOR_LESS_THAN 3
#define OPERATOR_LESS_EQUAL 4
#define OPERATOR_GREATER_THAN 5
#define OPERATOR_GREATER_EQUAL 6

typedef struct {
  uint8_t attribute_index;
  uint8_t operator;
  attribute_value_t value;
} proposition_t;

typedef struct {
  proposition_t* propositions;
  size_t proposition_count;
} selection_criteria_t;

typedef struct {
  char** column_names;
  attribute_value_t** rows;
  size_t row_count;
  size_t column_count;
} query_result_t;

/**
 * @brief Frees the memory allocated for a query result
 *
 * @param result Pointer to the query result to free
 */
void query_free_query_result(query_result_t* result);

/**
 * @brief Executes a SELECT query on the DBMS session with the given selection criteria
 *
 * @param session Pointer to the DBMS session
 * @param criteria Pointer to the selection criteria
 * @return Pointer to the query result, or NULL on failure
 * @note The caller is responsible for freeing the returned query_result_t using query_free_query_result()
 */
query_result_t* query_select(dbms_session_t* session, selection_criteria_t* criteria);

#endif  // QUERY_H