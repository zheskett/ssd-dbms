# quick benchmarking test of indexes

#!/bin/bash

./ssd-dbms-cli <<EOF
create bench.dat
id
1
name
3
32
finish
open bench.dat
bench fill 20000 0
time query select id = 12345; bench
bench index id
time query select id = 12345; bench
exit
EOF
