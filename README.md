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
./ssd-dbms-cli <--create|--read> <path_to_db_file>
```

Run tests

```bash
ctest --output-on-failure 
```

## The CLI

When you first run the CLI with `--create <path_to_db_file>`, it will create a new database file at the specified path. It will prompt you to enter the schema for the database table. It will then create the database file and initialize it with the provided schema. The CLI will then be as if run with `--read <path_to_db_file>`.
Subsequent runs with `--read <path_to_db_file>` will open the existing database file for reading and writing.

### Commands

- `insert <attribute1, attribute2, ...>`: Inserts a new record into the database. The values should be provided in the order of the schema defined during creation.
- `print catalog`: Prints the catalog of the database, showing the schema and metadata.
- `print page <page_id>`: Prints the contents of the specified page number in the database file. (page_id starts at 1)
- `print tuple <page_id> <slot_id>`: Prints the specified tuple from with the give tuple ID. (page_id starts at 1, slot_id starts at 0)
- `evict <page_id>`: Evicts the specified page from the buffer pool, writing it back to disk if it has been modified. (page_id starts at 1)
- `delete <page_id> <slot_id>`: Deletes the specified tuple from the database. (page_id starts at 1, slot_id starts at 0)
- `exit`: Exits the CLI.
