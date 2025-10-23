# ssd-dbms

Database management system designed for SSDs

## How to use:

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
