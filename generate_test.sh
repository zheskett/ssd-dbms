#!/bin/bash
open_string="open test1.dat\nopen test2.dat\nopen test3.dat\nopen test4.dat\nopen test5.dat\nopen test6.dat\n"
time_string_1="time split false test1 fill 100000 0;test2 fill 100000 0;test3 fill 100000 0\n"
time_string_2="time split true test4 fill 100000 0;test5 fill 100000 0;test6 fill 100000 0\n"
exit_string="exit\n"
rm -f test.dat test1.dat test2.dat test3.dat test4.dat test5.dat test6.dat
echo -e "create test.dat\nfield_1\n1\nfield_2\n2\nfield_3\n3\n32\nfield_4\n4\nfinish\nexit\n" | ./build/ssd-dbms-cli
cp test.dat test1.dat
cp test.dat test2.dat
cp test.dat test3.dat
cp test.dat test4.dat
cp test.dat test5.dat
cp test.dat test6.dat
#echo -e "$open_string$time_string_1$time_string_2$exit_string" | ./build/ssd-dbms-cli
