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
#include <sys/stat.h>
#include <unistd.h>

int ssdio_open(const char* filename, bool is_new) {
  // Open file with appropriate flags based on OS
  // 0644 says read/write for owner, read for group and others
  int fd = -1;
  int flags = O_RDWR | O_CLOEXEC | O_BINARY | (is_new ? O_CREAT | O_TRUNC : 0);
  int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
#if defined(ON_LINUX)
  fd = open(filename, flags | O_DIRECT, mode);
  if (fd >= 0) {
    // Try to not use read-ahead on Linux
    posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
  }
#elif defined(ON_MAC)
  fd = open(filename, flags, mode);
  if (fd >= 0) {
    // macOS does not support O_DIRECT; use fcntl to set F_NOCACHE
    fcntl(fd, F_NOCACHE, 1);
    fcntl(fd, F_RDAHEAD, 0);
  }
#else
  fd = open(filename, flags, mode);
#endif
  if (fd < 0) {
    return -1;
  }

  return fd;
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
  catalog->tuple_size = tuple_size + NULL_BYTE_SIZE;

  // Sort the records by attribute order
  for (int i = 0; i < ((int)catalog->record_count) - 1; i++) {
    for (int j = i + 1; j < ((int)catalog->record_count); j++) {
      if (catalog->records[i].attribute_order > catalog->records[j].attribute_order) {
        catalog_record_t temp = catalog->records[i];
        catalog->records[i] = catalog->records[j];
        catalog->records[j] = temp;
      }
    }
  }

  return true;
}

bool ssdio_write_catalog(int fd, const system_catalog_t* catalog) {
  // Buffer has to be aligned to 4096 because of O_DIRECT, just use PAGE_SIZE for simplicity
  catalog_record_t* buffer = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
  memset(buffer, 0, PAGE_SIZE);
  if (!buffer) {
    fprintf(stderr, "Failed to allocate aligned buffer for catalog\n");
    return false;
  }

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
  free(buffer);
  return bytes_written == PAGE_SIZE;
}

off_t ssdio_get_file_size(int fd) {
  struct stat s;
  if (fstat(fd, &s) == -1) {
    fprintf(stderr, "fstat(%d) failed\n", fd);
    return 0;
  }
  return (s.st_size);
}
