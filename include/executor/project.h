#ifndef PROJECT_H
#define PROJECT_H

#include "executor/executor.h"

// Forward declaration for TupleHashSet (defined in project.c)
typedef struct TupleHashSet TupleHashSet;

typedef struct {
    dbms_session_t* session;
    uint8_t* column_indices;       // Array of attribute indices to project
    uint8_t column_count;
    tuple_t projected_tuple;       // Reusable output tuple (shallow copy)
    attribute_value_t* projected_attrs;  // Pre-allocated attribute array for projection

    // DISTINCT support
    bool is_distinct;
    TupleHashSet* seen_tuples;     // Hash set for deduplication (NULL if not distinct)
} ProjectState;

/**
 * @brief Creates a Project operator for column subsetting
 *
 * @param child The child operator to project from
 * @param session Pointer to the DBMS session
 * @param column_indices Array of attribute indices to include in projection
 * @param column_count Number of columns to project
 * @param is_distinct If true, eliminate duplicate tuples
 * @return Pointer to the created operator, or NULL on failure
 */
Operator* project_create(Operator* child, dbms_session_t* session,
                         uint8_t* column_indices, uint8_t column_count,
                         bool is_distinct);

#endif /* PROJECT_H */

