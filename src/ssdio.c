/**
 * @file ssdio.c
 *
 * @author SSD-1
 * @brief Does IO operations for SSD-DBMS
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "ssdio.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int ssdio_open(const char* filename) {
  // Open file with appropriate flags based on OS
  // 0644 says read/write for owner, read for group and others
#if defined(ON_LINUX)
  int fd = open(filename, O_RDWR | O_CREAT | O_CLOEXEC | O_DIRECT | O_BINARY, 0644);
  if (fd != -1) {
    // Try to not use read-ahead on Linux
    posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM)
  }
  return fd;
#elif defined(ON_MAC)
  int fd = open(filename, O_RDWR | O_CREAT | O_CLOEXEC | O_BINARY, 0644);
  if (fd != -1) {
    // macOS does not support O_DIRECT; use fcntl to set F_NOCACHE
    fcntl(fd, F_NOCACHE, 1);
    fcntl(fd, F_RDAHEAD, 0);
  }
  return fd;
#else
  int fd = open(filename, O_RDWR | O_CREAT | O_CLOEXEC | O_BINARY, 0644);
  return fd;
#endif
}

int ssdio_close(int fd) {
  return close(fd);
}

int ssdio_flush(int fd) {
#if defined(ON_MAC)
  // On macOS, have to use F_FULLFSYNC for a full flush to disk
  int full_fsync = fcntl(fd, F_FULLFSYNC, 1);
  if (full_fsync != 0) {
    return full_fsync;
  }
#endif
  return fsync(fd);
}

bool ssdio_read_page(int fd, uint64_t page_id, page_t* page) {
  off_t offset = page_id * PAGE_SIZE;
  ssize_t bytes_read = pread(fd, page, PAGE_SIZE, offset);
  return bytes_read == PAGE_SIZE;
}

bool ssdio_write_page(int fd, uint64_t page_id, const page_t* page) {
  off_t offset = page_id * PAGE_SIZE;
  ssize_t bytes_written = pwrite(fd, page, PAGE_SIZE, offset);
  return bytes_written == PAGE_SIZE;
}

bool ssdio_read_catalog(int fd, system_catalog_t* catalog) {
  catalog_record_t buffer[PAGE_SIZE / sizeof(catalog_record_t)];
  ssize_t bytes_read = pread(fd, buffer, PAGE_SIZE, 0);
  if (bytes_read != PAGE_SIZE) {
    return false;
  }

  for (int i = 0; i < PAGE_SIZE / sizeof(catalog_record_t); i++) {
    // Check for end of valid records
    if (buffer[i].attribute_size == 0) {
      catalog->record_count = i;
      break;
    }
  }

  catalog->records = malloc(catalog->record_count * sizeof(catalog_record_t));
  if (!catalog->records) {
    return false;
  }

  uint16_t tuple_size = 0;
  for (int i = 0; i < catalog->record_count; i++) {
    catalog->records[i] = buffer[i];
    tuple_size += buffer[i].attribute_size;
  }

  // +1 for null byte
  catalog->tuple_size = tuple_size + 1;
  return true;
}

bool ssdio_write_catalog(int fd, const system_catalog_t* catalog) {
  catalog_record_t buffer[PAGE_SIZE / sizeof(catalog_record_t)] = {0};

  for (int i = 0; i < catalog->record_count; i++) {
    if (i >= PAGE_SIZE / sizeof(catalog_record_t)) {
      fprintf(stderr, "Catalog too large to write to a single page\n");
      return false;
    }

    if (catalog->records[i].attribute_size == 0) {
      fprintf(stderr, "Invalid catalog record with size 0\n");
      return false;
    }

    if (strnlen(catalog->records[i].attribute_name, CATALOG_ATTRIBUTE_NAME_SIZE) == 0) {
      fprintf(stderr, "Catalog record attribute name is invalid\n");
      return false;
    }

    buffer[i] = catalog->records[i];
  }

  ssize_t bytes_written = pwrite(fd, buffer, PAGE_SIZE, 0);
  return bytes_written == PAGE_SIZE;
}