#ifndef CLI_COMMANDS_H
#define CLI_COMMANDS_H

#define CLI_INSERT_COMMAND "insert"
#define CLI_PRINT_COMMAND "print"
#define CLI_EXIT_COMMAND "exit"
#define CLI_EVICT_COMMAND "evict"
#define CLI_DELETE_COMMAND "delete"

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

/**
 * @brief Executes the insert command
 *
 * @param session Pointer to the DBMS session
 * @param input_line Input line containing attribute values
 * @return CLI return code
 */
int cli_insert_command(dbms_session_t* session, char* input_line);

/**
 * @brief Executes the print command
 *
 * @param session Pointer to the DBMS session
 * @param input_line Input line containing print parameters
 * @return CLI return code
 */
int cli_print_command(dbms_session_t* session, char* input_line);

/**
 * @brief Executes the evict command
 *
 * @param session Pointer to the DBMS session
 * @param input_line Input line containing evict parameters
 * @return CLI return code
 */
int cli_evict_command(dbms_session_t* session, char* input_line);

/**
 * @brief Executes the delete command
 *
 * @param session Pointer to the DBMS session
 * @param input_line Input line containing delete parameters
 * @return CLI return code
 */
int cli_delete_command(dbms_session_t* session, char* input_line);

#endif /* CLI_COMMANDS_H */