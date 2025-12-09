#ifndef NESTED_LOOP_JOIN_H
#define NESTED_LOOP_JOIN_H

#include "executor/executor.h"

typedef struct {
    dbms_session_t* session;
    tuple_t* outer_tuple;              // Current tuple from outer relation
    bool outer_exhausted;              // Flag when outer is done

    // Combined output tuple
    tuple_t combined_tuple;
    attribute_value_t* combined_attrs;
    uint8_t outer_attr_count;
    uint8_t inner_attr_count;
} NestedLoopJoinState;

/**
 * @brief Creates a Nested Loop Join (Cross-Product) operator
 * NOTE: Since intermediate results don't have a catalog, we must manually
 * specify the number of attributes from each child to allocate memory correctly.
 *
 * @param outer The outer (left) child operator
 * @param inner The inner (right) child operator
 * @param session Pointer to the DBMS session
 * @param outer_column_count Number of attributes from outer relation
 * @param inner_column_count Number of attributes from inner relation
 * @return Pointer to the created operator, or NULL on failure
 */
Operator* nested_loop_join_create(Operator* outer, Operator* inner,
                                  dbms_session_t* session,
                                  uint8_t outer_column_count,
                                  uint8_t inner_column_count);

#endif /* NESTED_LOOP_JOIN_H */

