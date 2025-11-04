#include "data_structures.h"

#include <stdlib.h>

// FNV-1a hash function for 64-bit keys
// From https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
static uint64_t fnv1a_hash(uint64_t key) {
  uint64_t hash = FNV_OFFSET_BASIS_64;
  for (size_t i = 0; i < sizeof(uint64_t); i++) {
    uint8_t byte_of_data = (key >> (i * 8)) & 0xff;
    hash ^= byte_of_data;
    hash *= FNV_PRIME_64;
  }
  return hash;
}

/**
 * @brief Inserts a key-value pair into the linked list in sorted order
 *
 * @param head Pointer to the head of the linked list
 * @param key The key to insert
 * @param value The value to insert
 * @return true on success, false on failure
 */
static bool linked_list_insert(hash_node_t** head, uint64_t key, uint64_t value);

/**
 * @brief Deletes a key from the linked list
 *
 * @param head Pointer to the head of the linked list
 * @param key The key to delete
 * @return true if the key was found and deleted, false otherwise
 */
static bool linked_list_delete(hash_node_t** head, uint64_t key);

/**
 * @brief Retrieves a value by key from the linked list
 *
 * @param head Pointer to the head of the linked list
 * @param key The key to search for
 * @param value_out Pointer to store the retrieved value
 * @return true if the key is found and value is set, false otherwise
 */
static bool linked_list_get(hash_node_t** head, uint64_t key, uint64_t* value_out);

/**
 * @brief Frees the linked list
 *
 * @param head Pointer to the head of the linked list
 */
static void linked_list_free(hash_node_t** head);

hash_table_t* hash_table_init(size_t bucket_count) {
  if (bucket_count == 0) {
    return NULL;
  }

  hash_table_t* table = malloc(sizeof(hash_table_t));
  if (!table) {
    return NULL;
  }

  // Make bucket count a power of two for more efficient modulus
  size_t power_of_two_bucket_count = 1;
  while (power_of_two_bucket_count < bucket_count) {
    power_of_two_bucket_count <<= 1;
  }

  table->buckets = calloc(power_of_two_bucket_count, sizeof(hash_node_t*));
  if (!table->buckets) {
    free(table);
    return NULL;
  }

  table->bucket_count = power_of_two_bucket_count;
  return table;
}

void hash_table_free(hash_table_t* table) {
  if (!table) {
    return;
  }

  if (table->buckets) {
    for (size_t i = 0; i < table->bucket_count; i++) {
      linked_list_free(&table->buckets[i]);
    }
    free(table->buckets);
    table->buckets = NULL;
  }
  free(table);
}

bool hash_table_insert(hash_table_t* table, uint64_t key, uint64_t value) {
  if (!table || table->bucket_count == 0) {
    return false;
  }

  size_t bucket_index = fnv1a_hash(key) & (table->bucket_count - 1);
  return linked_list_insert(&table->buckets[bucket_index], key, value);
}

bool hash_table_delete(hash_table_t* table, uint64_t key) {
  if (!table || table->bucket_count == 0) {
    return false;
  }

  size_t bucket_index = fnv1a_hash(key) & (table->bucket_count - 1);
  return linked_list_delete(&table->buckets[bucket_index], key);
}

bool hash_table_get(hash_table_t* table, uint64_t key, uint64_t* value_out) {
  if (!table || table->bucket_count == 0) {
    return false;
  }

  size_t bucket_index = fnv1a_hash(key) & (table->bucket_count - 1);
  return linked_list_get(&table->buckets[bucket_index], key, value_out);
}

#pragma region Linked List Helper Functions
static bool linked_list_insert(hash_node_t** head, uint64_t key, uint64_t value) {
  if (!head) {
    return false;
  }

  hash_node_t* current = *head;
  hash_node_t* previous = NULL;

  while (current && current->key < key) {
    previous = current;
    current = current->next;
  }

  if (current && current->key == key) {
    current->value = value;
    return true;
  }

  hash_node_t* new_node = malloc(sizeof(hash_node_t));
  if (!new_node) {
    return false;
  }
  new_node->key = key;
  new_node->value = value;
  new_node->next = current;

  if (previous == NULL) {
    *head = new_node;
  } else {
    previous->next = new_node;
  }
  return true;
}

static bool linked_list_delete(hash_node_t** head, uint64_t key) {
  if (!head) {
    return false;
  }

  hash_node_t* current = *head;
  hash_node_t* previous = NULL;

  while (current && current->key < key) {
    previous = current;
    current = current->next;
  }

  if (!current || current->key != key) {
    return false;  // Key not found
  }

  if (previous == NULL) {
    *head = current->next;  // Deleting head
  } else {
    previous->next = current->next;  // Bypass the current node
  }

  free(current);
  return true;
}

static bool linked_list_get(hash_node_t** head, uint64_t key, uint64_t* value_out) {
  if (!head) {
    return false;
  }

  hash_node_t* current = *head;
  while (current && current->key < key) {
    current = current->next;
  }
  if (current && current->key == key) {
    if (value_out) {
      *value_out = current->value;
    }
    return true;
  }
  return false;
}

static void linked_list_free(hash_node_t** head) {
  if (!head) {
    return;
  }

  hash_node_t* current = *head;
  while (current) {
    hash_node_t* to_free = current;
    current = current->next;
    free(to_free);
  }
  *head = NULL;
}

#pragma endregion