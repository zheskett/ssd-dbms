#include "cli_commands.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pretty.h"

int cli_exec(dbms_session_t* session, char* input) {
  if (!session) {
    fprintf(stderr, "No DBMS session\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  if (!input) {
    fprintf(stderr, "No input command\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Convert input line into "tokens"
  // Tokens consist of command and arguments separated by whitespace
  char* command = strtok(input, " \t\n");

  if (!command) {
    fprintf(stderr, "No command entered\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Get the rest of the line as input_line
  char* input_line = strtok(NULL, "\n");

  // Match command
  if (strcmp(command, CLI_INSERT_COMMAND) == 0) {
    return cli_insert_command(session, input_line);
  } else if (strcmp(command, CLI_PRINT_COMMAND) == 0) {
    return cli_print_command(session, input_line);
  } else if (strcmp(command, CLI_EXIT_COMMAND) == 0) {
    return CLI_EXIT_RETURN_CODE;
  } else {
    fprintf(stderr, "Unknown command: %s\n", command);
    return CLI_FAILURE_RETURN_CODE;
  }
}

int cli_insert_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line (on commas)
  char* tokens[256];
  int token_count = 0;
  char* token = strtok(input_line, ",");
  while (token && token_count < 256) {
    tokens[token_count++] = token;
    token = strtok(NULL, ",");
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
        attributes[i].string_value = strdup(attribute_str);
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
  if (!dbms_insert_tuple(session, attributes)) {
    fprintf(stderr, "Failed to insert tuple into DBMS\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  return CLI_SUCCESS_RETURN_CODE;
}

int cli_print_command(dbms_session_t* session, char* input_line) {
  if (!session || !input_line) {
    fprintf(stderr, "Invalid session or input line\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Tokenize input line
  char* tokens[3];
  int token_count = 0;
  char* token = strtok(input_line, " \t\n");
  while (token && token_count < 3) {
    tokens[token_count++] = token;
    token = strtok(NULL, " \t\n");
  }
  if (token_count < 1) {
    fprintf(stderr, "Usage: print <catalog|page> [page_number] [print_nulls]\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  if (strcmp(tokens[0], "catalog") == 0) {
    print_catalog(session->catalog);
    return CLI_SUCCESS_RETURN_CODE;
  } else if (strcmp(tokens[0], "page") == 0) {
    if (token_count < 3) {
      fprintf(stderr, "Usage: print page <page_number> [print_nulls]\n");
      return CLI_FAILURE_RETURN_CODE;
    }
    uint64_t page_id = strtoull(tokens[1], NULL, 10);
    bool print_nulls = (token_count > 2) ? (strcmp(tokens[2], "true") == 0) : false;
    print_page(session, page_id, print_nulls);

    return CLI_SUCCESS_RETURN_CODE;
  }

  fprintf(stderr, "Unknown print target: %s\n", tokens[1]);
  return CLI_FAILURE_RETURN_CODE;
}