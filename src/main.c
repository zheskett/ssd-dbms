#include "main.h"

int main(int argc, char* argv[]) {
  // Read existing database
  // Make dbms manager
  dbms_manager_t* manager = dbms_init_dbms_manager();
  if (!manager) {
    fprintf(stderr, "Failed to initialize DBMS manager\n");
    return EXIT_FAILURE;
  }

  // Initialize linenoise
  linenoiseHistorySetMaxLen(HISTORY_SIZE);
  linenoiseSetMultiLine(1);

  // CLI loop
  while (true) {
    char* input = linenoise(CLI_PROMPT);
    if (input == NULL) {
      printf("Exiting CLI.\n");
      break;
    }

    if (strlen(input) == 0) {
      linenoiseFree(input);
      continue;
    }

    linenoiseHistoryAdd(input);

    int command_value = cli_exec(manager, input);

    linenoiseFree(input);

    if (command_value == CLI_EXIT_RETURN_CODE) {
      printf("Exiting CLI.\n");
      break;
    }
  }

  for (size_t i = 0; i < manager->session_count; i++) {
    dbms_flush_buffer_pool(manager->sessions[i]);
  }

  dbms_free_dbms_manager(manager);
  return EXIT_SUCCESS;
}
