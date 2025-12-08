# ssd-dbms

Database management system designed for SSDs

## How to run

Make build folder (run following commands in build/)

```bash
mkdir build; cd build
```

Configure (Debug mode)

```bash
cmake -S .. -B . -DCMAKE_BUILD_TYPE=Debug
```

Build

```bash
cmake --build .
```

Run the CLI

```bash
./ssd-dbms-cli
```

Run tests

```bash
ctest --output-on-failure 
```

## The CLI

| Command | Use |
|:-|:-|
| `create <table_path>` | Creates a new table at the specified path. You will be prompted to enter the schema for the table. |
| `open <table_path>` | Opens an existing table at the specified path. Will provide you the table name to use for subsequent commands. |
| `time <command>` | Times the execution of the specified command and prints the elapsed time. |
| `split <is_threaded> <command1>; <command2>; ...` | Splits the input commands into multiple commands to be executed in parallel. `is_threaded` should be true or false to indicate whether to use threading. Each command should be one that is prefixed with the table name it operates on, followed by a semicolon. (Maximum of 16 splits) |
| `query <query_command>` | Executes a query command. (More information below) |
| `<table_name> print catalog` | Prints the catalog of the database, showing the schema and metadata. |
| `<table_name> print page <page_id> [print_nulls]` | Prints the contents of the specified page number in the database file. (page_id starts at 1). `print_nulls` can be true or false (default is false) to indicate whether to print null values. |
| `<table_name> print tuple <page_id> <slot_id>` | Prints the specified tuple from with the give tuple ID. (page_id starts at 1, slot_id starts at 0) |
| `<table_name> insert <attribute1, attribute2, ...>` | Inserts a new record into the database. The values should be provided in the order of the schema defined during creation. |
| `<table_name> update <proposition1>; [<proposition2>; ...] \| <attribute1, attribute2, ...>` | Updates the specified tuple in the database with new attribute values. |
| `<table_name> delete <proposition1>; [<proposition2>; ...]` | Deletes the specified tuple from the database.  |
| `<table_name> fill <num_records> <start_number>` | Fills the database with the specified number of records. The records will have sequential values starting from `start_number`. |
| `<table_name> evict all` | Evicts the entire table from the buffer pool, writing back any modified pages to disk. |
| `<table_name> evict page <page_id>` | Evicts the specified page from the buffer pool, writing it back to disk if it has been modified. (page_id starts at 1) |
| `<table_name> index <attribute_name>` | Creates a hash index on the specified attribute (speeds up equality select queries) |
| `exit` | Exits the CLI. |

### Query Commands and Propositions

The `query` command allows you to execute queries on the database. The syntax for the query command is as follows:

`query <query_command>`

Where `<query_command>` is of the form:

#### Select Command

`select <proposition1>; [<proposition2>; ...] <table_name>`

Selects records from the database that satisfy the given propositions.

#### Propositions

Each proposition should be in the format:

`<attribute_name> <operator> <value>`

Where `<operator>` can be one of the following: `=`, `!=`, `<`, `>`, `<=`, `>=`.
DO NOT USE QUOTES FOR STRING VALUES IN PROPOSITIONS.
