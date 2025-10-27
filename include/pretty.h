#ifndef PRETTY_H
#define PRETTY_H

#include "dbms.h"

/**
 * @brief Prints the system catalog to stdout
 *
 * @param catalog Pointer to the system catalog to print
 */
void print_catalog(const system_catalog_t* catalog);

#endif /* PRETTY_H */