#include "cli_commands.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pretty.h"

int cli_exec_command(dbms_session_t* session, char* input) {
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
  char* tokens[4] = {0};
  int token_count = 0;
  char* token = strtok(input, " \t\n");
  while (token && token_count < 4) {
    tokens[token_count] = token;
    token_count++;
    token = strtok(NULL, " \t\n");
  }

  if (token_count == 0) {
    fprintf(stderr, "No command entered\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  // Match command
  if (strcmp(tokens[0], CLI_INSERT_COMMAND) == 0) {
    return cli_insert_command(session, token_count, tokens);
  } else if (strcmp(tokens[0], CLI_PRINT_COMMAND) == 0) {
    return cli_print_command(session, token_count, tokens);
  } else if (strcmp(tokens[0], CLI_EXIT_COMMAND) == 0) {
    return CLI_EXIT_RETURN_CODE;
  } else {
    fprintf(stderr, "Unknown command: %s\n", tokens[0]);
    return CLI_FAILURE_RETURN_CODE;
  }
}

int cli_insert_command(dbms_session_t* session, int argc, char* argv[]) {
  // Placeholder implementation
  printf("Insert command executed with %d arguments.\n", argc - 1);
  return CLI_SUCCESS_RETURN_CODE;
}

int cli_print_command(dbms_session_t* session, int argc, char* argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: print <catalog|page>\n");
    return CLI_FAILURE_RETURN_CODE;
  }

  if (strcmp(argv[1], "catalog") == 0) {
    print_catalog(session->catalog);
    return CLI_SUCCESS_RETURN_CODE;
  } else if (strcmp(argv[1], "page") == 0) {
    if (argc < 3) {
      fprintf(stderr, "Usage: print page <page_number> [print_nulls]\n");
      return CLI_FAILURE_RETURN_CODE;
    }
    uint64_t page_id = strtoull(argv[2], NULL, 10);
    bool print_nulls = (argc > 3) ? (strcmp(argv[3], "true") == 0) : false;
    print_page(session, page_id, print_nulls);

    return CLI_SUCCESS_RETURN_CODE;
  } else {
    fprintf(stderr, "Unknown print target: %s\n", argv[1]);
    return CLI_FAILURE_RETURN_CODE;
  }
}