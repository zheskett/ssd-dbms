#include "main.h"

int main(int argc, char* argv[]) {
  // Read existing database
  // Make dbms manager
  dbms_manager_t* manager = dbms_init_dbms_manager();
  if (!manager) {
    fprintf(stderr, "Failed to initialize DBMS manager\n");
    return EXIT_FAILURE;
  }

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

    int command_value = cli_exec(manager, input);
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
