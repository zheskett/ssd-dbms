#ifndef FILTER_H
#define FILTER_H

#include "executor/executor.h"
#include "query.h"

typedef struct {
    dbms_session_t* session;
    selection_criteria_t* criteria;
} FilterState;

/**
 * @brief Creates a Filter operator that applies a predicate to tuples
 *
 * @param child The child operator to filter
 * @param session Pointer to the DBMS session
 * @param criteria The selection criteria (predicates) to apply
 * @return Pointer to the created operator, or NULL on failure
 */
Operator* filter_create(Operator* child, dbms_session_t* session, selection_criteria_t* criteria);

#endif /* FILTER_H */

