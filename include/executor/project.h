#ifndef PROJECT_H
#define PROJECT_H

#include "executor/executor.h"

typedef struct {
    dbms_session_t* session;
    uint8_t* column_indices;       // Array of attribute indices to project
    uint8_t column_count;
    tuple_t projected_tuple;       // Reusable output tuple (shallow copy)
    attribute_value_t* projected_attrs;  // Pre-allocated attribute array for projection
} ProjectState;

/**
 * @brief Creates a Project operator for column subsetting
 *
 * @param child The child operator to project from
 * @param session Pointer to the DBMS session
 * @param column_indices Array of attribute indices to include in projection
 * @param column_count Number of columns to project
 * @return Pointer to the created operator, or NULL on failure
 */
Operator* project_create(Operator* child, dbms_session_t* session,
                         uint8_t* column_indices, uint8_t column_count);

#endif /* PROJECT_H */

