#ifndef MAIN_H
#define MAIN_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CLI_PROMPT "ssd-dbms> "
#define INPUT_BUFFER_SIZE 1024
#define HISTORY_SIZE 64

/**
 * @brief CLI entry point
 *
 * @return Exit status
 */
int main(int argc, char* argv[]);

#endif /* MAIN_H */
