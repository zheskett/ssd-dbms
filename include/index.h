#ifndef INDEX_H
#define INDEX_H

#include "dbms.h"

// Configuration for Lazy-Split Linear Hashing
// Split when load factor > 0.75 AND the split candidate chain > 3 nodes
#define LOAD_FACTOR_NUMERATOR 3
#define LOAD_FACTOR_DENOMINATOR 4
#define LAZY_SPLIT_THRESHOLD 3 

// Simple chained hash node for storing Tuple IDs
typedef struct index_node {
  uint64_t key;
  tuple_id_t tuple_id;
  struct index_node* next;
} index_node_t;

struct index {
  index_node_t** buckets;
  
  // Linear Hashing State
  size_t capacity;
  size_t bucket_count;          // Current number of active buckets (N)
  size_t initial_bucket_count;  // N at level 0
  size_t num_records;           // Total records in index
  size_t level;                 // Current level (L)
  size_t next_split;            // Split pointer (p)
};

/**
 * @brief Creates and populates an index for a specific attribute
 *
 * @param session The active session
 * @param attribute_index The index of the attribute to index
 * @return Pointer to the created index, or NULL on failure
 */
index_t* index_create(dbms_session_t* session, uint8_t attribute_index);

/**
 * @brief Frees the index memory
 * 
 * @param idx Pointer to the index
 */
void index_free(index_t* idx);

/**
 * @brief Inserts an entry into the index using Lazy-Split Linear Hashing
 * 
 * @param idx Pointer to the index
 * @param key Hash of the attribute value
 * @param tuple_id The tuple ID
 */
void index_insert(index_t* idx, uint64_t key, tuple_id_t tuple_id);

/**
 * @brief Removes a specific tuple entry from the index
 * 
 * @param idx Pointer to the index
 * @param key Hash of the attribute value
 * @param tuple_id The tuple ID to remove
 * @return true if found and removed
 */
bool index_delete(index_t* idx, uint64_t key, tuple_id_t tuple_id);

/**
 * @brief Retrieves all Tuple IDs matching the key
 * 
 * @param idx Pointer to the index
 * @param key Hash of the attribute value
 * @param out_count Pointer to store the number of results
 * @return Malloc'd array of tuple_id_t (caller must free), or NULL if none found
 */
tuple_id_t* index_lookup(index_t* idx, uint64_t key, size_t* out_count);

/**
 * @brief Helper to hash an attribute value
 * 
 * @param value Pointer to the attribute value
 * @return uint64_t hash
 */
uint64_t index_hash_attribute(const attribute_value_t* value);

#endif /* INDEX_H */
