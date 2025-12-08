#include "executor/seq_scan.h"

#include <stdlib.h>

// Forward declarations for iterator interface
static void seq_scan_open(Operator* self);
static tuple_t* seq_scan_next(Operator* self);
static void seq_scan_close(Operator* self);

Operator* seq_scan_create(dbms_session_t* session) {
  if (!session) {
    return NULL;
  }

  Operator* op = calloc(1, sizeof(Operator));
  if (!op) {
    return NULL;
  }

  SeqScanState* state = calloc(1, sizeof(SeqScanState));
  if (!state) {
    free(op);
    return NULL;
  }

  state->session = session;
  state->current_page_id = 0;
  state->current_slot_id = 0;
  state->tuples_per_page = dbms_catalog_tuples_per_page(session->catalog);
  state->current_buffer_page = NULL;

  op->state = state;
  op->open = seq_scan_open;
  op->next = seq_scan_next;
  op->close = seq_scan_close;
  op->destroy = NULL;  // No extra allocations to free
  op->children = NULL;
  op->child_count = 0;

  return op;
}

static void seq_scan_open(Operator* self) {
  if (!self || !self->state) {
    return;
  }

  SeqScanState* state = (SeqScanState*)self->state;

  // Initialize scan position
  state->current_page_id = 1;
  state->current_slot_id = 0;

  // Pin the first page if there are pages
  if (state->session->page_count > 0) {
    state->current_buffer_page = dbms_pin_page(state->session, state->current_page_id);
  } else {
    state->current_buffer_page = NULL;
  }
}

static tuple_t* seq_scan_next(Operator* self) {
  if (!self || !self->state) {
    return NULL;
  }

  SeqScanState* state = (SeqScanState*)self->state;

  // End of scan
  if (!state->current_buffer_page) {
    return NULL;
  }

  // Search for next non-null tuple
  while (state->current_buffer_page) {
    // Check current page for valid tuples
    while (state->current_slot_id < state->tuples_per_page) {
      tuple_t* tuple = &state->current_buffer_page->tuples[state->current_slot_id];
      state->current_slot_id++;

      if (!tuple->is_null) {
        return tuple;
      }
    }

    // Current page exhausted, move to next page
    // Unpin current page first (Pin-Scan-Unpin strategy)
    dbms_unpin_page(state->session, state->current_buffer_page);
    state->current_buffer_page = NULL;
    state->current_slot_id = 0;
    state->current_page_id++;

    // Check if there are more pages
    if (state->current_page_id <= state->session->page_count) {
      state->current_buffer_page = dbms_pin_page(state->session, state->current_page_id);
    }
  }

  return NULL;
}

static void seq_scan_close(Operator* self) {
  if (!self || !self->state) {
    return;
  }

  SeqScanState* state = (SeqScanState*)self->state;

  // Unpin any held page
  if (state->current_buffer_page) {
    dbms_unpin_page(state->session, state->current_buffer_page);
    state->current_buffer_page = NULL;
  }

  // Reset state
  state->current_page_id = 0;
  state->current_slot_id = 0;
}

