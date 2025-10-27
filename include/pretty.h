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
 * @brief Converts attribute type to string representation
 *
 * @param attribute_type The attribute type as uint8_t
 * @return String representation of the attribute type
 */
const char* attribute_type_to_string(uint8_t attribute_type);

#endif /* PRETTY_H */