#! /usr/bin/env python
# coding=utf-8

import sys

for i in range(2000):
    print("insert into my_table (id1,id2) values(%s, %s);"%(i,i+3))

#print("select * from my_table;")
print("exit;")