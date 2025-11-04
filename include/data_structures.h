#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define FNV_PRIME_64 0x100000001b3UL
#define FNV_OFFSET_BASIS_64 0xcbf29ce484222325UL

typedef struct hash_node {
  uint64_t key;
  uint64_t value;
  struct hash_node* next;
} hash_node_t;

typedef struct {
  hash_node_t** buckets;
  size_t bucket_count;
} hash_table_t;

/**
 * @brief Initializes a hash table
 *
 * @param bucket_count Number of buckets in the hash table
 * @return Pointer to the initialized hash table, or NULL on failure
 */
hash_table_t* hash_table_init(size_t bucket_count);

/**
 * @brief Frees a hash table
 *
 * @param table Pointer to the hash table to free
 */
void hash_table_free(hash_table_t* table);

/**
 * @brief Inserts a key-value pair into the hash table
 *
 * @param table Pointer to the hash table
 * @param key The key to insert
 * @param value The value to insert
 * @return true on success, false on failure
 */
bool hash_table_insert(hash_table_t* table, uint64_t key, uint64_t value);

/**
 * @brief Deletes a key from the hash table
 *
 * @param table Pointer to the hash table
 * @param key The key to delete
 * @return true if the key was found and deleted, false otherwise
 */
bool hash_table_delete(hash_table_t* table, uint64_t key);

/**
 * @brief Retrieves a value by key from the hash table
 *
 * @param table Pointer to the hash table
 * @param key The key to search for
 * @param value_out Pointer to store the retrieved value
 * @return true if the key is found and value is set, false otherwise
 */
bool hash_table_get(hash_table_t* table, uint64_t key, uint64_t* value_out);

#endif /* DATA_STRUCTURES_H */