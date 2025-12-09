#!/bin/bash

./ssd-dbms-cli <<EOF
create bench.db
id
1
name
3
32
finish
open bench.db
bench fill 200000 0
time query select id = 199999; bench
bench index id
time query select id = 199999; bench
exit
EOF
