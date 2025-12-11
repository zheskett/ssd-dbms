#include "index.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define INITIAL_BUCKETS 128

// Constants for the hashing
#define FNV_PRIME_64 0x100000001b3UL
#define FNV_OFFSET_BASIS_64 0xcbf29ce484222325UL

// "Panic" threshold (200% load factor)
#define PANIC_LOAD_NUMERATOR 2
#define PANIC_LOAD_DENOMINATOR 1

// Calculate chain length for Lazy-Split check
static size_t get_chain_length(index_node_t* head) {
    size_t count = 0;
    while (head) {
        count++;
        head = head->next;
    }
    return count;
}

static uint64_t hash_bytes(const void* data, size_t len) {
    uint64_t hash = FNV_OFFSET_BASIS_64;
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++) {
        hash ^= p[i];
        hash *= FNV_PRIME_64;
    }
    return hash;
}

uint64_t index_hash_attribute(const attribute_value_t* value) {
    if (!value) return 0;
    switch (value->type) {
        case ATTRIBUTE_TYPE_INT:
            return hash_bytes(&value->int_value, sizeof(value->int_value));
        case ATTRIBUTE_TYPE_FLOAT:
            return hash_bytes(&value->float_value, sizeof(value->float_value));
        case ATTRIBUTE_TYPE_BOOL:
            return hash_bytes(&value->bool_value, sizeof(value->bool_value));
        case ATTRIBUTE_TYPE_STRING:
            return hash_bytes(value->string_value, strlen(value->string_value));
        default:
            return 0;
    }
}

// Calculate the bucket address using Linear Hashing rules
static size_t get_bucket_address(index_t* idx, uint64_t key) {
    // H_L(k) = key % (2^L * initial_buckets)
    size_t multiplier = (1ULL << idx->level);
    size_t mask = (multiplier * idx->initial_bucket_count) - 1;
    size_t addr = key & mask;

    if (addr < idx->next_split) {
        // H_{L+1}(k) = key % (2^{L+1} * initial_buckets)
        size_t next_mask = ((multiplier << 1) * idx->initial_bucket_count) - 1;
        addr = key & next_mask;
    }
    return addr;
}

// Perform the Linear Hash split operation
static void perform_split(index_t* idx) {
    size_t split_idx = idx->next_split;
    
    // Determine index of the new bucket
    size_t multiplier = (1ULL << idx->level);
    size_t new_bucket_idx = split_idx + (multiplier * idx->initial_bucket_count);

    // Expand buckets array if necessary
    if (new_bucket_idx >= idx->capacity) {
        size_t new_capacity = idx->capacity * 2; // Double the size
        index_node_t** new_buckets = realloc(idx->buckets, new_capacity * sizeof(index_node_t*));
        if (!new_buckets) return; // Handle allocation failure
        
        // Zero out the newly allocated region
        memset(new_buckets + idx->capacity, 0, (new_capacity - idx->capacity) * sizeof(index_node_t*));
        
        idx->buckets = new_buckets;
        idx->capacity = new_capacity;
    }
    
    // Redistribute items from buckets[split_idx]
    index_node_t* current = idx->buckets[split_idx];
    idx->buckets[split_idx] = NULL; // Clear old bucket, will rebuild
    idx->buckets[new_bucket_idx] = NULL;

    while (current) {
        index_node_t* next = current->next;
        
        // Re-hash using the next level function H_{L+1}
        size_t next_mask = ((multiplier << 1) * idx->initial_bucket_count) - 1;
        size_t addr = current->key & next_mask;

        // Insert into appropriate bucket (either split_idx or new_bucket_idx)
        current->next = idx->buckets[addr];
        idx->buckets[addr] = current;

        current = next;
    }

    // Advance split pointer
    idx->next_split++;
    idx->bucket_count++; // Logically added one bucket

    // Check if round is complete
    if (idx->next_split >= (multiplier * idx->initial_bucket_count)) {
        idx->next_split = 0;
        idx->level++;
    }
}

index_t* index_create(dbms_session_t* session, uint8_t attribute_index) {
    if (!session) return NULL;

    index_t* idx = calloc(1, sizeof(index_t));
    if (!idx) return NULL;

    idx->initial_bucket_count = INITIAL_BUCKETS;
    idx->bucket_count = INITIAL_BUCKETS;
    idx->level = 0;
    idx->next_split = 0;
    idx->num_records = 0;

    idx->capacity = INITIAL_BUCKETS * 2; 
    
    idx->buckets = calloc(idx->capacity, sizeof(index_node_t*));
    if (!idx->buckets) {
        free(idx);
        return NULL;
    }

    // Populate index from existing data
    uint64_t tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
    for (uint64_t page_id = 1; page_id <= session->page_count; page_id++) {
        for (uint64_t tuple_index = 0; tuple_index < tuples_per_page; tuple_index++) {
            tuple_id_t tid = {page_id, tuple_index};
            tuple_t* tuple = dbms_get_tuple(session, tid);
            if (tuple && !tuple->is_null) {
                uint64_t key = index_hash_attribute(&tuple->attributes[attribute_index]);
                index_insert(idx, key, tid);
            }
        }
    }

    return idx;
}

void index_free(index_t* idx) {
    if (!idx) return;

    // 1. Free the chains (Linked Lists)
    for (size_t i = 0; i < idx->capacity; i++) {
        index_node_t* node = idx->buckets[i];
        while (node) {
            index_node_t* temp = node;
            node = node->next;
            free(temp); // Free the node
        }
    }

    // 2. Free the array and the struct
    free(idx->buckets);
    free(idx);
}

void index_insert(index_t* idx, uint64_t key, tuple_id_t tuple_id) {
    if (!idx) return;

    size_t bucket = get_bucket_address(idx, key);
    
    index_node_t* new_node = malloc(sizeof(index_node_t));
    if (!new_node) return;

    new_node->key = key;
    new_node->tuple_id = tuple_id;
    new_node->next = idx->buckets[bucket];
    idx->buckets[bucket] = new_node;
    idx->num_records++;
    
    size_t load_num = idx->num_records;
    size_t load_den = idx->bucket_count;

    // 1. Normal Trigger: Global Load > 75%
    bool high_load = (load_num * LOAD_FACTOR_DENOMINATOR) > (load_den * LOAD_FACTOR_NUMERATOR);
    
    // 2. Panic Trigger: Global Load > 200% (Safety Valve)
    bool panic_load = (load_num * PANIC_LOAD_DENOMINATOR) > (load_den * PANIC_LOAD_NUMERATOR);

    if (high_load) {
        // Check local condition
        size_t split_candidate_len = get_chain_length(idx->buckets[idx->next_split]);
        bool local_overflow = split_candidate_len >= LAZY_SPLIT_THRESHOLD;

        // Perform split if:
        // A) The candidate bucket is actually full
        // B) The table is too overfilled
        if (local_overflow || panic_load) {
            perform_split(idx);
        }
    }
}

bool index_delete(index_t* idx, uint64_t key, tuple_id_t tuple_id) {
    if (!idx) return false;
    
    size_t bucket = get_bucket_address(idx, key);
    index_node_t* curr = idx->buckets[bucket];
    index_node_t* prev = NULL;

    while (curr) {
        if (curr->key == key && 
            curr->tuple_id.page_id == tuple_id.page_id && 
            curr->tuple_id.slot_id == tuple_id.slot_id) {
            
            if (prev) {
                prev->next = curr->next;
            } else {
                idx->buckets[bucket] = curr->next;
            }
            free(curr);
            idx->num_records--;
            return true;
        }
        prev = curr;
        curr = curr->next;
    }
    return false;
}

tuple_id_t* index_lookup(index_t* idx, uint64_t key, size_t* out_count) {
    if (!idx || !out_count) return NULL;
    
    *out_count = 0;
    size_t bucket = get_bucket_address(idx, key);
    
    // First pass: count matches
    size_t count = 0;
    index_node_t* curr = idx->buckets[bucket];
    while (curr) {
        if (curr->key == key) {
            count++;
        }
        curr = curr->next;
    }

    if (count == 0) return NULL;

    tuple_id_t* results = malloc(count * sizeof(tuple_id_t));
    if (!results) return NULL;

    // Second pass: fill results
    size_t i = 0;
    curr = idx->buckets[bucket];
    while (curr) {
        if (curr->key == key) {
            results[i++] = curr->tuple_id;
        }
        curr = curr->next;
    }

    *out_count = count;
    return results;
}
