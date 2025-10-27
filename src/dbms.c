#include "dbms.h"

#include <stdlib.h>

void dbms_free_system_catalog(system_catalog_t* catalog) {
  if (catalog) {
    if (catalog->records) {
      free(catalog->records);
    }
    free(catalog);
  }
}