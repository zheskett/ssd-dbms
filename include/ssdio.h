#ifndef SSDIO_H
#define SSDIO_H

#include <stdbool.h>

#include "dbms.h"

#ifndef O_BINARY
#  define O_BINARY 0
#endif

#if defined(__linux__)
#  define ON_LINUX 1
#elif defined(__APPLE__)
#  define ON_MAC 1
#endif

/**
 * @brief Opens a file for SSD-DBMS
 *
 * @param filename Name of the file/table to open
 * @return File descriptor (-1 on failure)
 */
int ssdio_open(const char* filename);

/**
 * @brief Closes a file for SSD-DBMS
 *
 * @param fd File descriptor
 * @return Exit status
 */
int ssdio_close(int fd);

/**
 * @brief Flushes a file for SSD-DBMS
 *
 * @param fd File descriptor
 * @return Exit status
 */
int ssdio_flush(int fd);

/**
 * @brief Reads a page from SSD-DBMS
 *
 * @param fd File descriptor
 * @param page_id ID of the page to read
 * @param page Pointer to store the read page
 * @return true on success, false on failure
 */
bool ssdio_read_page(int fd, uint64_t page_id, page_t* page);

/**
 * @brief Writes a page to SSD-DBMS
 *
 * @param fd File descriptor
 * @param page_id ID of the page to write
 * @param page Pointer to the page to write
 * @return true on success, false on failure
 */
bool ssdio_write_page(int fd, uint64_t page_id, const page_t* page);

/**
 * @brief Reads the system catalog from SSD-DBMS
 *
 * @param fd File descriptor
 * @param catalog Pointer to store the read catalog
 * @return true on success, false on failure
 */
bool ssdio_read_catalog(int fd, system_catalog_t* catalog);

/**
 * @brief Writes the system catalog to SSD-DBMS
 *
 * @param fd File descriptor
 * @param catalog Pointer to the catalog to write
 * @return true on success, false on failure
 */
bool ssdio_write_catalog(int fd, const system_catalog_t* catalog);

#endif /* SSDIO_H */