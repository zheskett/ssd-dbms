#ifndef CLI_COMMANDS_H
#define CLI_COMMANDS_H

#define CLI_INSERT_COMMAND "insert"
#define CLI_PRINT_COMMAND "print"
#define CLI_EXIT_COMMAND "exit"

#define CLI_SUCCESS_RETURN_CODE 1
#define CLI_FAILURE_RETURN_CODE -1
#define CLI_EXIT_RETURN_CODE 0

#include "dbms.h"

/**
 * @brief Executes a CLI command
 *
 * @param session Pointer to the DBMS session
 * @param input Input command string
 * @return CLI return code
 */
int cli_exec(dbms_session_t* session, char* input);

int cli_insert_command(dbms_session_t* session, char* input_line);
int cli_print_command(dbms_session_t* session, char* input_line);

#endif /* CLI_COMMANDS_H */