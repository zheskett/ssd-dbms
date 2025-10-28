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
 * @param page Pointer to the page to print
 * @param catalog Pointer to the system catalog for interpreting tuples
 * @param print_nulls Whether to print NULL attributes
 */
void print_page(const page_t* page, const system_catalog_t* catalog, bool print_nulls);

/**
 * @brief Converts attribute type to string representation
 *
 * @param attribute_type The attribute type as uint8_t
 * @return String representation of the attribute type
 */
const char* attribute_type_to_string(uint8_t attribute_type);

#endif /* PRETTY_H */