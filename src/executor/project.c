#include "executor/project.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// TupleHashSet implementation for DISTINCT support
// ============================================================================

#define TUPLE_HASH_SET_BUCKETS 256
#define FNV_PRIME_64 0x100000001b3UL
#define FNV_OFFSET_BASIS_64 0xcbf29ce484222325UL

typedef struct TupleHashNode {
    uint64_t hash;                    // Pre-computed hash
    attribute_value_t* attrs;         // Deep copy of attribute values
    uint8_t attr_count;
    struct TupleHashNode* next;
} TupleHashNode;

struct TupleHashSet {
    TupleHashNode** buckets;
    size_t bucket_count;
};

static TupleHashSet* tuple_hash_set_init(size_t bucket_count);
static void tuple_hash_set_free(TupleHashSet* set);
static void tuple_hash_set_clear(TupleHashSet* set);
static uint64_t hash_tuple_attrs(const attribute_value_t* attrs, uint8_t count);
static bool attrs_equal(const attribute_value_t* a, const attribute_value_t* b, uint8_t count);
static bool tuple_hash_set_contains(TupleHashSet* set, const attribute_value_t* attrs, uint8_t count);
static bool tuple_hash_set_insert(TupleHashSet* set, const attribute_value_t* attrs, uint8_t count);

static TupleHashSet* tuple_hash_set_init(size_t bucket_count) {
    TupleHashSet* set = calloc(1, sizeof(TupleHashSet));
    if (!set) return NULL;

    set->buckets = calloc(bucket_count, sizeof(TupleHashNode*));
    if (!set->buckets) {
        free(set);
        return NULL;
    }

    set->bucket_count = bucket_count;
    return set;
}

static void tuple_hash_set_free(TupleHashSet* set) {
    if (!set) return;

    // Free all nodes and their deep-copied attributes
    for (size_t i = 0; i < set->bucket_count; i++) {
        TupleHashNode* node = set->buckets[i];
        while (node) {
            TupleHashNode* next = node->next;
            // Free deep-copied string attributes
            if (node->attrs) {
                for (uint8_t j = 0; j < node->attr_count; j++) {
                    if (node->attrs[j].type == ATTRIBUTE_TYPE_STRING && node->attrs[j].string_value) {
                        free(node->attrs[j].string_value);
                    }
                }
                free(node->attrs);
            }
            free(node);
            node = next;
        }
    }

    free(set->buckets);
    free(set);
}

static void tuple_hash_set_clear(TupleHashSet* set) {
    if (!set) return;

    // Free all nodes and their deep-copied attributes (but keep the set structure)
    for (size_t i = 0; i < set->bucket_count; i++) {
        TupleHashNode* node = set->buckets[i];
        while (node) {
            TupleHashNode* next = node->next;
            if (node->attrs) {
                for (uint8_t j = 0; j < node->attr_count; j++) {
                    if (node->attrs[j].type == ATTRIBUTE_TYPE_STRING && node->attrs[j].string_value) {
                        free(node->attrs[j].string_value);
                    }
                }
                free(node->attrs);
            }
            free(node);
            node = next;
        }
        set->buckets[i] = NULL;
    }
}

static uint64_t hash_tuple_attrs(const attribute_value_t* attrs, uint8_t count) {
    uint64_t hash = FNV_OFFSET_BASIS_64;

    for (uint8_t i = 0; i < count; i++) {
        const attribute_value_t* attr = &attrs[i];

        // Hash the type
        hash ^= (uint64_t)attr->type;
        hash *= FNV_PRIME_64;

        // Hash the value based on type
        switch (attr->type) {
            case ATTRIBUTE_TYPE_INT: {
                uint32_t val = (uint32_t)attr->int_value;
                for (int b = 0; b < 4; b++) {
                    hash ^= (val >> (b * 8)) & 0xFF;
                    hash *= FNV_PRIME_64;
                }
                break;
            }
            case ATTRIBUTE_TYPE_FLOAT: {
                // Treat float bits as uint32
                union { float f; uint32_t u; } conv;
                conv.f = attr->float_value;
                for (int b = 0; b < 4; b++) {
                    hash ^= (conv.u >> (b * 8)) & 0xFF;
                    hash *= FNV_PRIME_64;
                }
                break;
            }
            case ATTRIBUTE_TYPE_STRING:
                if (attr->string_value) {
                    for (const char* p = attr->string_value; *p; p++) {
                        hash ^= (uint8_t)*p;
                        hash *= FNV_PRIME_64;
                    }
                }
                break;
            case ATTRIBUTE_TYPE_BOOL:
                hash ^= attr->bool_value ? 1 : 0;
                hash *= FNV_PRIME_64;
                break;
            default:
                break;
        }
    }

    return hash;
}

static bool attrs_equal(const attribute_value_t* a, const attribute_value_t* b, uint8_t count) {
    for (uint8_t i = 0; i < count; i++) {
        if (a[i].type != b[i].type) return false;

        switch (a[i].type) {
            case ATTRIBUTE_TYPE_INT:
                if (a[i].int_value != b[i].int_value) return false;
                break;
            case ATTRIBUTE_TYPE_FLOAT:
                if (a[i].float_value != b[i].float_value) return false;
                break;
            case ATTRIBUTE_TYPE_STRING:
                if (a[i].string_value && b[i].string_value) {
                    if (strcmp(a[i].string_value, b[i].string_value) != 0) return false;
                } else if (a[i].string_value != b[i].string_value) {
                    return false;  // One is NULL, other is not
                }
                break;
            case ATTRIBUTE_TYPE_BOOL:
                if (a[i].bool_value != b[i].bool_value) return false;
                break;
            default:
                break;
        }
    }
    return true;
}

static bool tuple_hash_set_contains(TupleHashSet* set, const attribute_value_t* attrs, uint8_t count) {
    if (!set || !attrs) return false;

    uint64_t hash = hash_tuple_attrs(attrs, count);
    size_t bucket = hash % set->bucket_count;

    TupleHashNode* node = set->buckets[bucket];
    while (node) {
        if (node->hash == hash && node->attr_count == count && attrs_equal(node->attrs, attrs, count)) {
            return true;
        }
        node = node->next;
    }

    return false;
}

static bool tuple_hash_set_insert(TupleHashSet* set, const attribute_value_t* attrs, uint8_t count) {
    if (!set || !attrs) return false;

    uint64_t hash = hash_tuple_attrs(attrs, count);
    size_t bucket = hash % set->bucket_count;

    // Create new node with deep copy of attributes
    TupleHashNode* node = calloc(1, sizeof(TupleHashNode));
    if (!node) return false;

    node->hash = hash;
    node->attr_count = count;
    node->attrs = calloc(count, sizeof(attribute_value_t));
    if (!node->attrs) {
        free(node);
        return false;
    }

    // Deep copy attributes
    for (uint8_t i = 0; i < count; i++) {
        node->attrs[i].type = attrs[i].type;
        switch (attrs[i].type) {
            case ATTRIBUTE_TYPE_INT:
                node->attrs[i].int_value = attrs[i].int_value;
                break;
            case ATTRIBUTE_TYPE_FLOAT:
                node->attrs[i].float_value = attrs[i].float_value;
                break;
            case ATTRIBUTE_TYPE_STRING:
                if (attrs[i].string_value) {
                    node->attrs[i].string_value = strdup(attrs[i].string_value);
                    if (!node->attrs[i].string_value) {
                        // Cleanup on failure
                        for (uint8_t j = 0; j < i; j++) {
                            if (node->attrs[j].type == ATTRIBUTE_TYPE_STRING && node->attrs[j].string_value) {
                                free(node->attrs[j].string_value);
                            }
                        }
                        free(node->attrs);
                        free(node);
                        return false;
                    }
                }
                break;
            case ATTRIBUTE_TYPE_BOOL:
                node->attrs[i].bool_value = attrs[i].bool_value;
                break;
            default:
                break;
        }
    }

    // Insert at head of bucket chain
    node->next = set->buckets[bucket];
    set->buckets[bucket] = node;

    return true;
}

// ============================================================================
// Project operator implementation
// ============================================================================

// Forward declarations for iterator interface
static void project_open(Operator* self);
static tuple_t* project_next(Operator* self);
static void project_close(Operator* self);
static void project_reset(Operator* self);
static void project_destroy(Operator* self);

Operator* project_create(Operator* child, dbms_session_t* session,
                         uint8_t* column_indices, uint8_t column_count,
                         bool is_distinct) {
    if (!child || !session || !column_indices || column_count == 0) {
        return NULL;
    }

    Operator* op = calloc(1, sizeof(Operator));
    if (!op) {
        return NULL;
    }

    ProjectState* state = calloc(1, sizeof(ProjectState));
    if (!state) {
        free(op);
        return NULL;
    }

    state->session = session;
    state->column_count = column_count;
    state->is_distinct = is_distinct;
    state->seen_tuples = NULL;

    // Copy the column indices array
    state->column_indices = calloc(column_count, sizeof(uint8_t));
    if (!state->column_indices) {
        free(state);
        free(op);
        return NULL;
    }
    memcpy(state->column_indices, column_indices, column_count * sizeof(uint8_t));

    // Allocate the projected attributes array
    state->projected_attrs = calloc(column_count, sizeof(attribute_value_t));
    if (!state->projected_attrs) {
        free(state->column_indices);
        free(state);
        free(op);
        return NULL;
    }

    // Initialize TupleHashSet if DISTINCT is enabled
    if (is_distinct) {
        state->seen_tuples = tuple_hash_set_init(TUPLE_HASH_SET_BUCKETS);
        if (!state->seen_tuples) {
            free(state->projected_attrs);
            free(state->column_indices);
            free(state);
            free(op);
            return NULL;
        }
    }

    // Initialize the projected tuple
    state->projected_tuple.id.page_id = 0;
    state->projected_tuple.id.slot_id = 0;
    state->projected_tuple.is_null = false;
    state->projected_tuple.attributes = state->projected_attrs;

    op->state = state;
    op->open = project_open;
    op->next = project_next;
    op->close = project_close;
    op->reset = project_reset;
    op->destroy = project_destroy;

    // Set up child relationship
    op->children = calloc(1, sizeof(Operator*));
    if (!op->children) {
        if (state->seen_tuples) tuple_hash_set_free(state->seen_tuples);
        free(state->projected_attrs);
        free(state->column_indices);
        free(state);
        free(op);
        return NULL;
    }
    op->children[0] = child;
    op->child_count = 1;

    return op;
}

static void project_open(Operator* self) {
    if (!self || !self->children || self->child_count < 1) {
        return;
    }

    // Open the child operator
    Operator* child = self->children[0];
    if (child && child->open) {
        child->open(child);
    }
}

static tuple_t* project_next(Operator* self) {
    if (!self || !self->state || !self->children || self->child_count < 1) {
        return NULL;
    }

    ProjectState* state = (ProjectState*)self->state;
    Operator* child = self->children[0];

    if (!child || !child->next) {
        return NULL;
    }

    // Loop to handle DISTINCT - skip duplicates
    while (true) {
        // Get the next tuple from the child
        tuple_t* tuple = child->next(child);
        if (!tuple) {
            return NULL;
        }

        // Copy tuple ID for reference
        state->projected_tuple.id = tuple->id;
        state->projected_tuple.is_null = tuple->is_null;

        // Project selected columns (shallow copy - pointers to buffer data)
        for (uint8_t i = 0; i < state->column_count; i++) {
            uint8_t src_idx = state->column_indices[i];
            // Copy the attribute value (for primitives this is a value copy,
            // for strings this copies the pointer - true zero-copy)
            state->projected_attrs[i] = tuple->attributes[src_idx];
        }

        // If DISTINCT, check hash set for duplicates
        if (state->is_distinct && state->seen_tuples) {
            if (tuple_hash_set_contains(state->seen_tuples, state->projected_attrs, state->column_count)) {
                continue;  // Skip duplicate, get next tuple
            }
            // Not a duplicate - add to seen set
            tuple_hash_set_insert(state->seen_tuples, state->projected_attrs, state->column_count);
        }

        return &state->projected_tuple;
    }
}

static void project_close(Operator* self) {
    if (!self || !self->children || self->child_count < 1) {
        return;
    }

    // Close the child operator
    Operator* child = self->children[0];
    if (child && child->close) {
        child->close(child);
    }
}

static void project_reset(Operator* self) {
    if (!self || !self->state || !self->children || self->child_count < 1) {
        return;
    }

    ProjectState* state = (ProjectState*)self->state;

    // Reset the child operator
    Operator* child = self->children[0];
    if (child && child->reset) {
        child->reset(child);
    }

    // Clear the TupleHashSet if DISTINCT is enabled (free strings to avoid memory leak)
    if (state->is_distinct && state->seen_tuples) {
        tuple_hash_set_clear(state->seen_tuples);
    }
}

static void project_destroy(Operator* self) {
    if (!self || !self->state) {
        return;
    }

    ProjectState* state = (ProjectState*)self->state;

    // Free allocated arrays within the state
    if (state->column_indices) {
        free(state->column_indices);
        state->column_indices = NULL;
    }

    if (state->projected_attrs) {
        free(state->projected_attrs);
        state->projected_attrs = NULL;
    }

    // Free TupleHashSet if DISTINCT was enabled
    if (state->seen_tuples) {
        tuple_hash_set_free(state->seen_tuples);
        state->seen_tuples = NULL;
    }
}
