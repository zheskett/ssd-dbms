#ifndef PRETTY_H
#define PRETTY_H

#include "dbms.h"

/**
 * @brief Prints the system catalog to stdout
 *
 * @param catalog Pointer to the system catalog to print
 */
void print_catalog(const system_catalog_t* catalog);

/**
 * @brief Prints the contents of a page to stdout
 *
 * @param session Pointer to the DBMS session
 * @param page_id ID of the page to print
 * @param print_nulls Whether to print NULL attributes
 */
void print_page(dbms_session_t* session, uint64_t page_id, bool print_nulls);

/**
 * @brief Prints the contents of a tuple to stdout
 *
 * @param session Pointer to the DBMS session
 * @param tuple Pointer to the tuple to print
 */
void print_tuple(dbms_session_t* session, tuple_t* tuple);

/**
 * @brief Converts attribute type to string representation
 *
 * @param attribute_type The attribute type as uint8_t
 * @return String representation of the attribute type
 */
const char* attribute_type_to_string(uint8_t attribute_type);

#endif /* PRETTY_H */