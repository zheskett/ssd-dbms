#ifndef CLI_COMMANDS_H
#define CLI_COMMANDS_H

#define CLI_INSERT_COMMAND "insert"
#define CLI_PRINT_COMMAND "print"
#define CLI_EXIT_COMMAND "exit"
#define CLI_EVICT_COMMAND "evict"
#define CLI_DELETE_COMMAND "delete"

#define CLI_CREATE_TABLE_COMMAND "create"
#define CLI_OPEN_TABLE_COMMAND "open"
#define CLI_SPLIT_COMMAND "split"
#define CLI_TIME_COMMAND "time"
#define CLI_FILL_COMMAND "fill"

#define MAX_SPLITS 16

#define CLI_SUCCESS_RETURN_CODE 1
#define CLI_FAILURE_RETURN_CODE -1
#define CLI_EXIT_RETURN_CODE 0

#include <pthread.h>

#include "dbms.h"

typedef struct {
  dbms_session_t* session;
  char* command_line;
  pthread_mutex_t* table_mutex;
} cli_thread_arg_t;

/**
 * @brief Executes a CLI command
 *
 * @param manager Pointer to the DBMS manager
 * @param input Input command string
 * @return CLI return code
 */
int cli_exec(dbms_manager_t* manager, char* input);

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

/**
 * @brief Executes the fill command
 *
 * @param session Pointer to the DBMS session
 * @param input_line Input line containing fill parameters
 * @return CLI return code
 */
int cli_fill_command(dbms_session_t* session, char* input_line);

/**
 * @brief Creates a new table via CLI
 *
 * @param manager Pointer to the DBMS manager
 * @param input_line Input line (not used)
 * @return CLI return code
 */
int cli_create_table_command(dbms_manager_t* manager, const char* input_line);

/**
 * @brief Opens an existing table via CLI
 *
 * @param manager Pointer to the DBMS manager
 * @param input_line Input line containing the table filename
 * @return CLI return code
 */
int cli_open_command(dbms_manager_t* manager, const char* input_line);

/**
 * @brief Runs commands in parallel, separated by ;
 *
 * @param manager Pointer to the DBMS manager
 * @param input_line Input line containing split parameters
 * @return CLI return code
 */
int cli_split_command(dbms_manager_t* manager, char* input_line);

/**
 * @brief Times the execution of a command
 *
 * @param manager Pointer to the DBMS manager
 * @param input_line Input line containing the command to time
 * @return CLI return code
 */
int cli_time_command(dbms_manager_t* manager, char* input_line);

#endif /* CLI_COMMANDS_H */