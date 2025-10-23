# ssd-dbms

Database management system designed for SSDs

How to use:
```bash
# Make build folder
mkdir build; cd build

# Configure (Debug mode)
cmake -S .. -B . -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build .

# Run the CLI
./ssd-dbms-cli

# Run all tests
ctest --output-on-failure 
```
