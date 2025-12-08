#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "dbms.h"

typedef struct Operator Operator;

struct Operator {
    void* state;                          // Private operator state

    // Iterator interface (function pointers for polymorphism)
    void  (*open)(Operator* self);
    tuple_t* (*next)(Operator* self);     // Zero-copy: returns pointer to buffer pool
    void  (*close)(Operator* self);

    // Cleanup function for operator-specific state (called by operator_free)
    void  (*destroy)(Operator* self);

    Operator** children;                  // Child operators (NULL for leaf nodes)
    int child_count;
};

// Convenience macros
#define OP_OPEN(op)  ((op)->open(op))
#define OP_NEXT(op)  ((op)->next(op))
#define OP_CLOSE(op) ((op)->close(op))

/**
 * @brief Frees an operator and its children recursively
 *
 * @param op Pointer to the operator to free
 */
void operator_free(Operator* op);

#endif /* EXECUTOR_H */

