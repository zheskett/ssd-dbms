#ifndef SEQ_SCAN_H
#define SEQ_SCAN_H

#include "executor/executor.h"

typedef struct {
    dbms_session_t* session;
    uint64_t current_page_id;
    uint64_t current_slot_id;
    uint64_t tuples_per_page;
    buffer_page_t* current_buffer_page;  // Currently pinned page
} SeqScanState;

/**
 * @brief Creates a SeqScan operator for sequential table scanning
 *
 * @param session Pointer to the DBMS session
 * @return Pointer to the created operator, or NULL on failure
 */
Operator* seq_scan_create(dbms_session_t* session);

#endif /* SEQ_SCAN_H */

