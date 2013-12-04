#! /usr/bin/env python
# coding=utf-8


import sys
import re
import commands
import db

db = db.db("db.txt")

reg = re.compile("(\.|,|\)|\(|;)", flags=re.U)
spaces = re.compile("\s")
create_flag = False
insert_flag = False
select_flag = False
fields_flag = False
values_flag = False
size_flag = False

exit_flag = False
field = {}
value = ''
command = 0


while not exit_flag:
    line = sys.stdin.readline()
    line = reg.sub(lambda c:  ' '+str(c.group())+' ', str(line))

    for word in line.split(' '):
        word = word.strip()
        if len(word) < 1:
            continue
        word = word.upper()
        #commands
        if not select_flag and not insert_flag and not create_flag:
            if word == 'CREATE':
                create_flag= True
                continue
            if word == 'INSERT':
                insert_flag = True
                continue
            if word == 'SELECT':
                select_flag = True
                fields_flag = True
                command = commands.Select(db)
                continue

        if select_flag:
            if fields_flag:
                if word == '*':
                    command.fields = []
                if word == 'FROM':
                    fields_flag = False
                else:
                    command.fields.append(word)
                continue
            elif word == ';':
                command.run()
                select_flag = False

            else:
                command.name = word

            continue

        if word == 'TABLE' and create_flag:
            create_flag = False
            command = commands.CreateTable(db)
            continue

        if word == 'INTO' and insert_flag:
            insert_flag = False
            command = commands.Insert(db)
            continue
        if word == 'EXIT':
            command = 'exit'
            continue
        if command == 0:
            exit_flag = True
            break
        if word == 'VALUES':
            values_flag = True;
            continue
        if word == '(' and not fields_flag and not values_flag:
            fields_flag = True
            continue
        if word == '(' and fields_flag:
            size_flag = True
            continue
        if word == ')' and size_flag:
            size_flag = False
            continue
        if word == ')' and not size_flag and fields_flag and not values_flag:
            command.fields.append(field)
            field = {}
            fields_flag = False
            continue
        if word == ')' and not fields_flag and values_flag:
            command.values.append(value) #todo avoid values capitalization
            value = ''                  #todo remove kavychkas in strings
            values_flag= False
            continue
        if word == ';':
            if command == 'exit':
                exit_flag = True
                break
            command.run()
            command = 0
            fields_flag = False
            create_flag = False
            size_flag = False
            continue
        if size_flag:
            field['size'] = word
            continue
        if word == ',' and fields_flag:
            command.fields.append(field)
            size_flag = False
            field = {}
            continue
        if word == ',' and values_flag:
            command.values.append(value)
            size_flag = False
            value = ''
            continue
        if not fields_flag and values_flag:
            value = word
            continue
        #todo fix float point parsing
        if fields_flag and not size_flag:
            if word in ['INT', 'DOUBLE', 'VARCHAR']:
                field['type'] = word
            else:
                field['name'] = word
            continue
        if not fields_flag and not fields_flag:
            command.name = word
            continue

db.close()