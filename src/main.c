#include "main.h"

static bool create_database(const char* filename);

int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <-read|--create> <database_file>\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char* mode = argv[1];
  if (strcmp(mode, "--read") != 0 && strcmp(mode, "--create") != 0) {
    fprintf(stderr, "Invalid mode: %s. Use --read or --create.\n", mode);
    return EXIT_FAILURE;
  }

  if (strcmp(mode, "--create") == 0) {
    if (!create_database(argv[2])) {
      return EXIT_FAILURE;
    }
  }

  // Read existing database
  // Open DBMS session
  dbms_session_t* session = dbms_init_dbms_session(argv[2]);
  if (!session) {
    fprintf(stderr, "Failed to initialize DBMS session for file: %s\n", argv[2]);
    return EXIT_FAILURE;
  }

  printf("Database '%s' opened successfully.\n", session->table_name);

  // CLI loop
  char input[1024];
  while (true) {
    printf("ssd-dbms> ");
    if (!fgets(input, sizeof(input), stdin)) {
      printf("Error reading input. Exiting.\n");
      break;
    }

    // Remove trailing newline
    input[strcspn(input, "\n")] = '\0';

    int command_value = cli_exec(session, input);
    if (command_value == CLI_EXIT_RETURN_CODE) {
      printf("Exiting CLI.\n");
      break;
    }
  }

  dbms_flush_buffer_pool(session);

  dbms_free_dbms_session(session);
  return EXIT_SUCCESS;
}

static bool create_database(const char* filename) {
  system_catalog_t* catalog = calloc(1, sizeof(system_catalog_t));
  if (!catalog) {
    fprintf(stderr, "Memory allocation failed for system catalog\n");
    return false;
  }
  catalog->records = NULL;
  // First byte determines if a record is null
  catalog->tuple_size = NULL_BYTE_SIZE;
  catalog->record_count = 0;

  // Let user define schema
  while (true) {
    char attribute_name[CATALOG_ATTRIBUTE_NAME_SIZE];
    uint8_t attribute_size;
    uint8_t attribute_type;

    printf("Enter attribute name (or 'finish' to finish): ");
    fgets(attribute_name, sizeof(attribute_name), stdin);
    attribute_name[strcspn(attribute_name, "\n")] = '\0';
    if (strcmp(attribute_name, "finish") == 0) {
      break;
    }
    if (strlen(attribute_name) == 0) {
      fprintf(stderr, "Attribute name cannot be empty\n");
      continue;
    }
    if (strcmp(attribute_name, PADDING_NAME) == 0) {
      fprintf(stderr, "Attribute name cannot be '%s'\n", PADDING_NAME);
      continue;
    }
    if (strchr(attribute_name, ' ') || strchr(attribute_name, '\t')) {
      fprintf(stderr, "Attribute name cannot contain whitespace\n");
      continue;
    }
    if (dbms_get_catalog_record_by_name(catalog, attribute_name) != NULL) {
      fprintf(stderr, "Attribute name '%s' already exists in catalog\n", attribute_name);
      continue;
    }

    char buffer[16] = {0};
    printf("Enter attribute type (1=INT, 2=FLOAT, 3=STRING, 4=BOOL): ");
    fgets(buffer, sizeof(buffer), stdin);
    attribute_type = (uint8_t)strtoul(buffer, NULL, 10);

    if (attribute_type == ATTRIBUTE_TYPE_STRING) {
      for (int i = 0; i < 16; i++) {
        buffer[i] = '\0';
      }
      printf("Enter attribute size (in bytes): ");
      fgets(buffer, sizeof(buffer), stdin);
      attribute_size = (uint8_t)strtoul(buffer, NULL, 10);
      if (attribute_size == 0) {
        fprintf(stderr, "Attribute size must be greater than 0\n");
        continue;
      }
    }

    // Check validity of attribute
    if (attribute_type < ATTRIBUTE_TYPE_INT || attribute_type > ATTRIBUTE_TYPE_BOOL) {
      fprintf(stderr, "Invalid attribute type: %d\n", attribute_type);
      continue;
    }
    if (attribute_type == ATTRIBUTE_TYPE_INT) {
      attribute_size = sizeof(int32_t);
    } else if (attribute_type == ATTRIBUTE_TYPE_FLOAT) {
      attribute_size = sizeof(float);
    } else if (attribute_type == ATTRIBUTE_TYPE_BOOL) {
      attribute_size = sizeof(bool);
    }

    catalog->record_count++;
    catalog->tuple_size += attribute_size;
    if (catalog->records == NULL) {
      catalog->records = calloc(1, sizeof(catalog_record_t));
    } else {
      catalog->records = realloc(catalog->records, catalog->record_count * sizeof(catalog_record_t));
    }
    if (!catalog->records) {
      fprintf(stderr, "Memory allocation failed for catalog records\n");
      dbms_free_system_catalog(catalog);
      return false;
    }

    catalog_record_t* record = &catalog->records[catalog->record_count - 1];
    memset(record, 0, sizeof(catalog_record_t));
    strncpy(record->attribute_name, attribute_name, CATALOG_ATTRIBUTE_NAME_SIZE - 1);
    record->attribute_size = attribute_size;
    record->attribute_type = attribute_type;
    record->attribute_order = catalog->record_count - 1;
  }

  if (catalog->record_count == 0) {
    fprintf(stderr, "No attributes defined. Aborting database creation.\n");
    dbms_free_system_catalog(catalog);
    return false;
  }

  // Fit records to 8-byte alignment, minimum 16 bytes
  if (catalog->tuple_size % 8 != 0 || catalog->tuple_size < 16) {
    uint16_t remaining = 8 - (catalog->tuple_size % 8);
    if (catalog->tuple_size < 16) {
      remaining = 16 - catalog->tuple_size;
    }
    // Make a padding attribute
    catalog->record_count++;
    catalog->tuple_size += remaining;
    catalog->records = realloc(catalog->records, catalog->record_count * sizeof(catalog_record_t));
    if (!catalog->records) {
      fprintf(stderr, "Memory allocation failed for catalog records\n");
      dbms_free_system_catalog(catalog);
      return false;
    }
    catalog_record_t* padding_record = &catalog->records[catalog->record_count - 1];
    memset(padding_record, 0, sizeof(catalog_record_t));
    strncpy(padding_record->attribute_name, PADDING_NAME, CATALOG_ATTRIBUTE_NAME_SIZE - 1);
    padding_record->attribute_size = remaining;
    padding_record->attribute_type = ATTRIBUTE_TYPE_UNUSED;
    padding_record->attribute_order = catalog->record_count - 1;
  }

  dbms_create_db(filename, catalog);

  printf("Database created successfully: %s\n", filename);
  dbms_free_system_catalog(catalog);
  return true;
}