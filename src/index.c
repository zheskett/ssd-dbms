#include "index.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define INITIAL_BUCKETS 1024

// Constants for the hashing
#define FNV_PRIME_64 0x100000001b3UL
#define FNV_OFFSET_BASIS_64 0xcbf29ce484222325UL

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

index_t* index_create(dbms_session_t* session, uint8_t attribute_index) {
    if (!session) return NULL;

    index_t* idx = calloc(1, sizeof(index_t));
    if (!idx) return NULL;

    idx->bucket_count = INITIAL_BUCKETS; 
    idx->buckets = calloc(idx->bucket_count, sizeof(index_node_t*));
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
    for (size_t i = 0; i < idx->bucket_count; i++) {
        index_node_t* node = idx->buckets[i];
        while (node) {
            index_node_t* temp = node;
            node = node->next;
            free(temp);
        }
    }
    free(idx->buckets);
    free(idx);
}

void index_insert(index_t* idx, uint64_t key, tuple_id_t tuple_id) {
    if (!idx) return;
        
    size_t bucket = key & (idx->bucket_count - 1);
    index_node_t* new_node = malloc(sizeof(index_node_t));
    if (!new_node) return;

    new_node->key = key;
    new_node->tuple_id = tuple_id;
    new_node->next = idx->buckets[bucket];
    idx->buckets[bucket] = new_node;
    idx->count++;
}

bool index_delete(index_t* idx, uint64_t key, tuple_id_t tuple_id) {
    if (!idx) return false;
    
    size_t bucket = key & (idx->bucket_count - 1);
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
            idx->count--;
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
    size_t bucket = key & (idx->bucket_count - 1);
    
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
