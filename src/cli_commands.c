#include "cli_commands.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pretty.h"

static int cli_table_exec(dbms_session_t* session, char* input_line) {
  char* save_ptr = NULL;
  char* command = strtok_r(input_line, " \t\n", &save_ptr);
  input_line = strtok_r(NULL, "\n", &save_ptr);

  if (strcmp(command, CLI_INSERT_COMMAND) == 0) {
    return cli_insert_command(session, input_line);
  } else if (strcmp(command, CLI_PRINT_COMMAND) == 0) {
    return cli_print_command(session, input_line);
  } else if (strcmp(command, CLI_EXIT_COMMAND) == 0) {
    return CLI_EXIT_RETURN_CODE;
  } else if (strcmp(command, CLI_EVICT_COMMAND) == 0) {
    return cli_evict_command(session, input_line);
  } else if (strcmp(command, CLI_DELETE_COMMAND) == 0) {
    return cli_delete_command(session, input_line);
  } else if (strcmp(command, CLI_FILL_COMMAND) == 0) {
    return cli_fill_command(session, input_line);
  } else {
    fprintf(stderr, "Unknown command: %s\n", command);
    return CLI_FAILURE_RETURN_CODE;
  }
}

static dbms_session_t* get_session_by_name(dbms_manager_t* manager, const char* table_name) {
  for (size_t i = 0; i < manager->session_count; i++) {
    if (strcmp(manager->sessions[i]->table_name, table_name) == 0) {
      return manager->sessions[i];
    }
  }
  return NULL;
}

static void* cli_thread_func(void* arg) {
  if (!arg) {
    fprintf(stderr, "Invalid thread argument\n");
    return NULL;
  }

  cli_thread_arg_t* thread_arg = (cli_thread_arg_t*)arg;
  pthread_mutex_t* table_mutex = thread_arg->table_mutex;
  pthread_mutex_lock(table_mutex);
  cli_table_exec(thread_arg->session, thread_arg->command_line);
  pthread_mutex_unlock(table_mutex);
  return NULL;
}

int cli_exec(dbms_manager_t* manager, char* input) {
  if (!manager) {
    fprintf(stderr, "No DBMS manager\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  if (!input) {
    fprintf(stderr, "No input command\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Convert input line into "tokens"
  // Tokens consist of command and arguments separated by whitespace
  char* save_ptr = NULL;
  char* command = strtok_r(input, " \t\n", &save_ptr);

  if (!command) {
    fprintf(stderr, "No command entered\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Get the rest of the line as input_line
  char* input_line = strtok_r(NULL, "\n", &save_ptr);

  // Match command
  if (strcmp(command, CLI_CREATE_TABLE_COMMAND) == 0) {
    return cli_create_table_command(manager, input_line);
  } else if (strcmp(command, CLI_EXIT_COMMAND) == 0) {
    return CLI_EXIT_RETURN_CODE;
  } else if (strcmp(command, CLI_OPEN_TABLE_COMMAND) == 0) {
    return cli_open_command(manager, input_line);
  } else if (strcmp(command, CLI_SPLIT_COMMAND) == 0) {
    return cli_split_command(manager, input_line);
  } else if (strcmp(command, CLI_TIME_COMMAND) == 0) {
    return cli_time_command(manager, input_line);
  }

  // For other commands, the first token is the table name
  if (input_line == NULL) {
    fprintf(stderr, "No input line provided for: %s\n", command);
    return CLI_FAILURE_RETURN_CODE;
  }
  const char* table_name = command;
  dbms_session_t* session = get_session_by_name(manager, table_name);
  if (!session) {
    fprintf(stderr, "Table '%s' not found in DBMS manager\n", table_name);
    return CLI_FAILURE_RETURN_CODE;
  }

  return cli_table_exec(session, input_line);
}

int cli_insert_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line (on commas)
  char* save_ptr = NULL;
  char* tokens[256];
  int token_count = 0;
  char* token = strtok_r(input_line, ",", &save_ptr);
  while (token && token_count < 256) {
    tokens[token_count++] = token;
    token = strtok_r(NULL, ",", &save_ptr);
  }

  // Convert tokens to attribute values
  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);
  if (token_count != num_attributes) {
    fprintf(stderr, "Attribute count mismatch: expected %u, got %d\n", num_attributes, token_count);
    return CLI_FAILURE_RETURN_CODE;
  }

  attribute_value_t attributes[num_attributes];
  for (int i = 0; i < num_attributes; i++) {
    uint8_t attribute_type = dbms_get_catalog_record(session->catalog, i)->attribute_type;
    char* attribute_str = tokens[i];
    attributes[i].type = attribute_type;

    switch (attribute_type) {
      case ATTRIBUTE_TYPE_INT:
        attributes[i].int_value = atoi(attribute_str);
        break;
      case ATTRIBUTE_TYPE_FLOAT:
        attributes[i].float_value = (float)atof(attribute_str);
        break;
      case ATTRIBUTE_TYPE_STRING:
        attributes[i].string_value = attribute_str;
        break;
      case ATTRIBUTE_TYPE_BOOL:
        attributes[i].bool_value = (bool)(strcmp(attribute_str, "true") == 0 || strcmp(attribute_str, "1") == 0);
        break;
      default:
        fprintf(stderr, "Unknown attribute type: %u\n", attribute_type);
        return CLI_FAILURE_RETURN_CODE;
    }
  }

  // Insert tuple into DBMS
  tuple_t* inserted_tuple = dbms_insert_tuple(session, attributes);
  if (!inserted_tuple) {
    fprintf(stderr, "Failed to insert tuple into DBMS\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  printf("TID (%llu, %llu) (", inserted_tuple->id.page_id, inserted_tuple->id.slot_id);
  for (int i = 0; i < num_attributes; i++) {
    if (i > 0) {
      printf(", ");
    }
    switch (inserted_tuple->attributes[i].type) {
      case ATTRIBUTE_TYPE_INT:
        printf("%d", inserted_tuple->attributes[i].int_value);
        break;
      case ATTRIBUTE_TYPE_FLOAT:
        printf("%f", inserted_tuple->attributes[i].float_value);
        break;
      case ATTRIBUTE_TYPE_STRING:
        printf("%.*s", (int)dbms_get_catalog_record(session->catalog, (uint8_t)i)->attribute_size,
               inserted_tuple->attributes[i].string_value);
        break;
      case ATTRIBUTE_TYPE_BOOL:
        printf("%s", inserted_tuple->attributes[i].bool_value ? "true" : "false");
        break;
    }
  }
  printf(") inserted\n");

  return CLI_SUCCESS_RETURN_CODE;
}

int cli_print_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line
  char* save_ptr = NULL;
  char* tokens[3];
  int token_count = 0;
  char* token = strtok_r(input_line, " \t\n", &save_ptr);
  while (token && token_count < 3) {
    tokens[token_count++] = token;
    token = strtok_r(NULL, " \t\n", &save_ptr);
  }
  if (token_count < 1) {
    fprintf(stderr, "Usage: print <catalog|page|tuple>\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  if (strcmp(tokens[0], "catalog") == 0) {
    print_catalog(session->catalog);
    return CLI_SUCCESS_RETURN_CODE;
  } else if (strcmp(tokens[0], "page") == 0) {
    if (token_count < 2) {
      fprintf(stderr, "Usage: print page <page_number> [print_nulls]\n");
      return CLI_FAILURE_RETURN_CODE;
    }
    uint64_t page_id = strtoull(tokens[1], NULL, 10);
    bool print_nulls = (token_count > 2) ? (strcmp(tokens[2], "true") == 0) : false;
    print_page(session, page_id, print_nulls);

    return CLI_SUCCESS_RETURN_CODE;
  } else if (strcmp(tokens[0], "tuple") == 0) {
    if (token_count < 3) {
      fprintf(stderr, "Usage: print tuple <page_number> <slot_number>\n");
      return CLI_FAILURE_RETURN_CODE;
    }
    uint64_t page_id = strtoull(tokens[1], NULL, 10);
    uint64_t slot_id = strtoull(tokens[2], NULL, 10);
    tuple_id_t tuple_id = {page_id, slot_id};
    tuple_t* tuple = dbms_get_tuple(session, tuple_id);
    if (!tuple) {
      fprintf(stderr, "Tuple (%llu, %llu) not found\n", page_id, slot_id);
      return CLI_FAILURE_RETURN_CODE;
    }
    print_tuple(session, tuple);

    return CLI_SUCCESS_RETURN_CODE;
  }

  fprintf(stderr, "Unknown print target: %s\n", tokens[0]);
  return CLI_FAILURE_RETURN_CODE;
}

int cli_evict_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line
  char* save_ptr = NULL;
  char* token = strtok_r(input_line, " \t\n", &save_ptr);
  if (!token) {
    fprintf(stderr, "Usage: evict <page_number>\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  uint64_t page_id = strtoull(token, NULL, 10);
  if (page_id <= 0 || page_id > session->page_count) {
    fprintf(stderr, "Invalid page number: %llu\n", page_id);
    return CLI_FAILURE_RETURN_CODE;
  }

  // Check if page is in buffer pool
  uint64_t buffer_page_index = 0;
  if (!hash_table_get(session->buffer_pool->page_table, page_id, &buffer_page_index)) {
    fprintf(stderr, "Page %llu is not in buffer pool\n", page_id);
    return CLI_FAILURE_RETURN_CODE;
  }
  buffer_page_t* buffer_page = &session->buffer_pool->buffer_pages[buffer_page_index];

  // Evict page from buffer pool
  dbms_flush_buffer_page(session, buffer_page, true);
  printf("Page %llu evicted from buffer pool\n", page_id);

  return CLI_SUCCESS_RETURN_CODE;
}

int cli_delete_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line
  char* save_ptr = NULL;
  char* tokens[2];
  int token_count = 0;
  char* token = strtok_r(input_line, " \t\n", &save_ptr);
  while (token && token_count < 2) {
    tokens[token_count++] = token;
    token = strtok_r(NULL, " \t\n", &save_ptr);
  }
  if (token_count < 2) {
    fprintf(stderr, "Usage: delete <page_number> <slot_number>\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  uint64_t page_id = strtoull(tokens[0], NULL, 10);
  uint64_t slot_id = strtoull(tokens[1], NULL, 10);
  tuple_id_t tuple_id = {page_id, slot_id};

  if (!dbms_delete_tuple(session, tuple_id)) {
    fprintf(stderr, "Failed to delete tuple (%llu, %llu)\n", page_id, slot_id);
    return CLI_FAILURE_RETURN_CODE;
  }

  printf("Tuple (%llu, %llu) deleted successfully\n", page_id, slot_id);
  return CLI_SUCCESS_RETURN_CODE;
}

int cli_fill_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line
  char* save_ptr = NULL;
  char* tokens[2];
  int token_count = 0;
  char* token = strtok_r(input_line, " \t\n", &save_ptr);
  while (token && token_count < 2) {
    tokens[token_count++] = token;
    token = strtok_r(NULL, " \t\n", &save_ptr);
  }
  if (token_count < 2) {
    fprintf(stderr, "Usage: fill <num_records> <start_number>\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  int64_t num_records = strtoll(tokens[0], NULL, 10);
  int64_t start_number = strtoll(tokens[1], NULL, 10);
  if (num_records <= 0) {
    fprintf(stderr, "Number of records must be greater than 0\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (num_records > 1000000) {
    fprintf(stderr, "Number of records cannot exceed 1,000,000\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  uint8_t num_attributes = dbms_catalog_num_used(session->catalog);
  attribute_value_t attributes[num_attributes];
  for (int64_t i = 0; i < num_records; i++) {
    for (uint8_t j = 0; j < num_attributes; j++) {
      uint8_t attribute_type = dbms_get_catalog_record(session->catalog, j)->attribute_type;
      attributes[j].type = attribute_type;

      switch (attribute_type) {
        case ATTRIBUTE_TYPE_INT:
          attributes[j].int_value = (int32_t)(start_number + i);
          break;
        case ATTRIBUTE_TYPE_FLOAT:
          attributes[j].float_value = (float)(start_number + i);
          break;
        case ATTRIBUTE_TYPE_STRING: {
          char buffer[32];
          snprintf(buffer, sizeof(buffer), "str_%lld", start_number + i);
          attributes[j].string_value = strdup(buffer);
          break;
        }
        case ATTRIBUTE_TYPE_BOOL:
          attributes[j].bool_value = ((start_number + i) % 2 == 0);
          break;
        default:
          fprintf(stderr, "Unknown attribute type: %u\n", attribute_type);
          return CLI_FAILURE_RETURN_CODE;
      }
    }

    tuple_t* inserted_tuple = dbms_insert_tuple(session, attributes);
    // Free string attributes
    for (uint8_t j = 0; j < num_attributes; j++) {
      if (attributes[j].type == ATTRIBUTE_TYPE_STRING) {
        free(attributes[j].string_value);
      }
    }

    if (!inserted_tuple) {
      fprintf(stderr, "Failed to insert tuple into DBMS\n");
      return CLI_FAILURE_RETURN_CODE;
    }
  }

  printf("%lld records inserted starting from %lld\n", num_records, start_number);
  return CLI_SUCCESS_RETURN_CODE;
}

int cli_create_table_command(dbms_manager_t* manager, const char* input_line) {
  if (!manager) {
    fprintf(stderr, "Invalid manager\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (!input_line) {
    fprintf(stderr, "No input line provided for create command\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  // Assume input_line is the filename
  const char* filename = input_line;
  if (strlen(filename) == 0) {
    fprintf(stderr, "Database filename cannot be empty\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (strchr(filename, ' ') || strchr(filename, '\t') || strchr(filename, '\n')) {
    fprintf(stderr, "Database filename cannot contain whitespace\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  system_catalog_t catalog = {0};
  catalog.records = NULL;
  // First byte determines if a record is null
  catalog.tuple_size = NULL_BYTE_SIZE;
  catalog.record_count = 0;

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
    if (strchr(attribute_name, ' ') || strchr(attribute_name, '\t') || strchr(attribute_name, '\n')) {
      fprintf(stderr, "Attribute name cannot contain whitespace\n");
      continue;
    }
    if (dbms_get_catalog_record_by_name(&catalog, attribute_name) != NULL) {
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
      long input_size = strtol(buffer, NULL, 10);
      attribute_size = (uint8_t)input_size;
      if (input_size <= 0) {
        fprintf(stderr, "Attribute size must be greater than 0\n");
        continue;
      }
      if (input_size > UINT8_MAX) {
        fprintf(stderr, "Attribute size cannot exceed %d bytes\n", UINT8_MAX);
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

    catalog.record_count++;
    catalog.tuple_size += attribute_size;
    catalog.records = realloc(catalog.records, catalog.record_count * sizeof(catalog_record_t));

    if (!catalog.records) {
      fprintf(stderr, "Memory allocation failed for catalog records\n");
      dbms_free_catalog_records(catalog);
      return CLI_FAILURE_RETURN_CODE;
    }

    catalog_record_t* record = &catalog.records[catalog.record_count - 1];
    memset(record, 0, sizeof(catalog_record_t));
    strncpy(record->attribute_name, attribute_name, CATALOG_ATTRIBUTE_NAME_SIZE - 1);
    record->attribute_size = attribute_size;
    record->attribute_type = attribute_type;
    record->attribute_order = catalog.record_count - 1;
  }

  if (catalog.record_count == 0) {
    fprintf(stderr, "No attributes defined. Aborting database creation.\n");
    dbms_free_catalog_records(catalog);
    return CLI_FAILURE_RETURN_CODE;
  }

  // Fit records to 8-byte alignment, minimum 16 bytes
  if (catalog.tuple_size % 8 != 0 || catalog.tuple_size < 16) {
    uint16_t remaining = 8 - (catalog.tuple_size % 8);
    if (catalog.tuple_size < 16) {
      remaining = 16 - catalog.tuple_size;
    }
    // Make a padding attribute
    catalog.record_count++;
    catalog.tuple_size += remaining;
    catalog.records = realloc(catalog.records, catalog.record_count * sizeof(catalog_record_t));
    if (!catalog.records) {
      fprintf(stderr, "Memory allocation failed for catalog records\n");
      dbms_free_catalog_records(catalog);
      return CLI_FAILURE_RETURN_CODE;
    }
    catalog_record_t* padding_record = &catalog.records[catalog.record_count - 1];
    memset(padding_record, 0, sizeof(catalog_record_t));
    strncpy(padding_record->attribute_name, PADDING_NAME, CATALOG_ATTRIBUTE_NAME_SIZE - 1);
    padding_record->attribute_size = remaining;
    padding_record->attribute_type = ATTRIBUTE_TYPE_UNUSED;
    padding_record->attribute_order = catalog.record_count - 1;
  }

  dbms_create_table(filename, &catalog);

  printf("Table created successfully: %s\n", filename);
  printf("Use '%s %s' to open the table.\n", CLI_OPEN_TABLE_COMMAND, filename);
  dbms_free_catalog_records(catalog);
  return CLI_SUCCESS_RETURN_CODE;
}

int cli_open_command(dbms_manager_t* manager, const char* input_line) {
  if (!manager) {
    fprintf(stderr, "Invalid manager\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (!input_line) {
    fprintf(stderr, "No input line provided for open command\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  const char* filename = input_line;
  if (strlen(filename) == 0) {
    fprintf(stderr, "Table filename cannot be empty\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  dbms_session_t* session = dbms_init_dbms_session(filename);
  if (!session) {
    fprintf(stderr, "Failed to open table: %s\n", filename);
    return CLI_FAILURE_RETURN_CODE;
  }

  dbms_add_session(manager, session);

  printf("Table '%s' opened successfully.\n", session->table_name);
  return CLI_SUCCESS_RETURN_CODE;
}

int cli_split_command(dbms_manager_t* manager, char* input_line) {
  if (!manager) {
    fprintf(stderr, "Invalid manager\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (!input_line) {
    fprintf(stderr, "No input line provided for split command\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // First token should be is_threaded
  char* save_ptr = NULL;
  char* is_threaded_str = strtok_r(input_line, " \t\n", &save_ptr);
  if (!is_threaded_str) {
    fprintf(stderr, "No threading flag provided for split command\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  bool is_threaded = (strcmp(is_threaded_str, "true") == 0 || strcmp(is_threaded_str, "1") == 0);

  // Split input_line by ';' and execute each command
  char* commands[MAX_SPLITS];
  int command_count = 0;
  char* token = strtok_r(NULL, ";", &save_ptr);
  while (token && command_count < MAX_SPLITS) {
    commands[command_count++] = token;
    token = strtok_r(NULL, ";", &save_ptr);
  }

  if (command_count == 0) {
    fprintf(stderr, "No commands provided for split\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (token != NULL) {
    fprintf(stderr, "Exceeded maximum number of splits (%d)\n", MAX_SPLITS);
    return CLI_FAILURE_RETURN_CODE;
  }

  dbms_session_t* table_map[MAX_SPLITS] = {0};
  char* table_names[MAX_SPLITS] = {0};
  for (int i = 0; i < command_count; i++) {
    char* command_line = commands[i];
    // Trim leading and trailing whitespace
    while (*command_line == ' ' || *command_line == '\t' || *command_line == '\n') {
      command_line++;
    }
    char* end = command_line + strlen(command_line) - 1;
    while (end > command_line && (*end == ' ' || *end == '\t' || *end == '\n')) {
      *end = '\0';
      end--;
    }

    // Split command cannot be another split, create, open, or time
    if (strncmp(command_line, CLI_SPLIT_COMMAND, strlen(CLI_SPLIT_COMMAND)) == 0 ||
        strncmp(command_line, CLI_CREATE_TABLE_COMMAND, strlen(CLI_CREATE_TABLE_COMMAND)) == 0 ||
        strncmp(command_line, CLI_OPEN_TABLE_COMMAND, strlen(CLI_OPEN_TABLE_COMMAND)) == 0 ||
        strncmp(command_line, CLI_TIME_COMMAND, strlen(CLI_TIME_COMMAND)) == 0) {
      fprintf(stderr, "Nested split, create, open, or time commands are not allowed\n");
      return CLI_FAILURE_RETURN_CODE;
    }

    // Extract table name
    char* save_ptr_inner = NULL;
    char* table_name = strtok_r(command_line, " \t\n", &save_ptr_inner);
    if (!table_name) {
      fprintf(stderr, "No table name provided in command: %s\n", command_line);
      return CLI_FAILURE_RETURN_CODE;
    }
    // Check if table already in table_map
    bool table_exists = false;
    for (int j = 0; j < i; j++) {
      if (table_map[j] && strcmp(table_map[j]->table_name, table_name) == 0) {
        table_exists = true;
        break;
      }
    }
    if (!table_exists) {
      table_map[i] = get_session_by_name(manager, table_name);
    }
    commands[i] = strtok_r(NULL, "\n", &save_ptr_inner);
    table_names[i] = table_name;
  }

  if (!is_threaded) {
    // Execute commands sequentially
    for (int i = 0; i < command_count; i++) {
      char* command_line = commands[i];
      char* table_name = table_names[i];
      int table_index = -1;
      for (int j = 0; j <= i; j++) {
        if (table_map[j] && strcmp(table_map[j]->table_name, table_name) == 0) {
          table_index = j;
          break;
        }
      }
      if (table_index == -1 || !table_map[table_index]) {
        fprintf(stderr, "Table '%s' not found for command: %s\n", table_name, commands[i]);
        return CLI_FAILURE_RETURN_CODE;
      }
      cli_table_exec(table_map[table_index], command_line);
    }
    return CLI_SUCCESS_RETURN_CODE;
  }

  // Execute commands in parallel
  // We cannot execute multiple commands on the same table simultaneously
  pthread_t threads[command_count];
  cli_thread_arg_t thread_args[command_count];
  pthread_mutex_t table_mutexes[command_count];
  // Createing more mutexes than necessary, but simplifies indexing
  for (int i = 0; i < command_count; i++) {
    pthread_mutex_init(&table_mutexes[i], NULL);
  }

  for (int i = 0; i < command_count; i++) {
    char* command_line = commands[i];
    char* table_name = table_names[i];
    int table_index = -1;
    for (int j = 0; j <= i; j++) {
      if (table_map[j] && strcmp(table_map[j]->table_name, table_name) == 0) {
        table_index = j;
        break;
      }
    }
    if (table_index == -1 || !table_map[table_index]) {
      fprintf(stderr, "Table '%s' not found for command: %s\n", table_name, commands[i]);
      return CLI_FAILURE_RETURN_CODE;
    }

    thread_args[i].session = table_map[table_index];
    thread_args[i].table_mutex = &table_mutexes[table_index];
    thread_args[i].command_line = command_line;
    if (pthread_create(&threads[i], NULL, cli_thread_func, &thread_args[i]) != 0) {
      fprintf(stderr, "Failed to create thread for command: %s\n", commands[i]);
      return CLI_FAILURE_RETURN_CODE;
    }
  }

  for (int i = 0; i < command_count; i++) {
    pthread_join(threads[i], NULL);
  }

  for (int i = 0; i < command_count; i++) {
    pthread_mutex_destroy(&table_mutexes[i]);
  }

  return CLI_SUCCESS_RETURN_CODE;
}

int cli_time_command(dbms_manager_t* manager, char* input_line) {
  if (!manager) {
    fprintf(stderr, "Invalid manager\n");
    return CLI_FAILURE_RETURN_CODE;
  }
  if (!input_line) {
    fprintf(stderr, "No input line provided for time command\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  clock_t start_time = clock();
  int result = cli_exec(manager, input_line);
  clock_t end_time = clock();
  if (result != CLI_SUCCESS_RETURN_CODE) {
    return result;
  }

  double elapsed_time = (double)(end_time - start_time) / (double)CLOCKS_PER_SEC;
  printf("Command executed in %.5f seconds\n", elapsed_time);

  return CLI_SUCCESS_RETURN_CODE;
}