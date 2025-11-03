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

- `create <table_path>`: Creates a new table at the specified path. You will be prompted to enter the schema for the table.
- `open <table_path>`: Opens an existing table at the specified path. Will provide you the table name to use for subsequent commands.
- `<table_name> insert <attribute1, attribute2, ...>`: Inserts a new record into the database. The values should be provided in the order of the schema defined during creation.
- `<table_name> print catalog`: Prints the catalog of the database, showing the schema and metadata.
- `<table_name> print page <page_id>`: Prints the contents of the specified page number in the database file. (page_id starts at 1)
- `<table_name> print tuple <page_id> <slot_id>`: Prints the specified tuple from with the give tuple ID. (page_id starts at 1, slot_id starts at 0)
- `<table_name> evict <page_id>`: Evicts the specified page from the buffer pool, writing it back to disk if it has been modified. (page_id starts at 1)
- `<table_name> delete <page_id> <slot_id>`: Deletes the specified tuple from the database. (page_id starts at 1, slot_id starts at 0)
- `exit`: Exits the CLI.
