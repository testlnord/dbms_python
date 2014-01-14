#! /usr/bin/env python
# coding=utf-8


import re
import parser
import db



db = db.db("db.txt")

pp = parser.Parser(db)

while True:
    try:
        command = pp.next()
        if not command is None:
            command.run()
    except SyntaxError:
        print("Syntax Error occurred")
    except StopIteration:
        break

db.close()