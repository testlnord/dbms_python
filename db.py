#! /usr/bin/env python
# coding=utf-8



import memman as mm
import struct


class db:
    def __init__(self, path):
        self.tables = []
        self.memman = mm.Memman(path+'db')


        #len = struct.unpack('i',page1.buffer[0:4])[0]
        #b = bitarray(page1.buffer[4:len//8+5])
        #page1.clean = False
        #page2 = self.memman.get_page(1)
        #page1.clean = False
        #page3 = self.memman.get_page(2)
        #page3.clean = False

        with open(path,'r') as db_file:
            table_count = int(db_file.readline())
            for ti in range(table_count):
                table = {}
                table['name'] = db_file.readline().strip()
                table['fields'] = []
                field_count = int(db_file.readline())
                for fi in range(field_count):
                    raw_field = db_file.readline().strip().split(' ')
                    field = {'name': raw_field[0], 'type': raw_field[1]}
                    if len(raw_field) > 2:
                        field['size'] = raw_field[2]
                    table['fields'].append(field)
                table['fmt'] = db._make_fmt(table['fields'])
                self.tables.append(table)
        self.path = path
        pass

    @staticmethod
    def _make_fmt(table_fields):
        fmt = ''
        for f in table_fields:
            if f['type'] == 'INT':
                fmt += 'i'
            elif f['type'] == 'DOUBLE':
                fmt += 'd'
            elif f['type'] == 'VARCHAR':
                fmt += str(f['size']) + 's'
        return fmt

    def add(self,table):
        self.tables.append(table)

    def insert(self, command):
        self.current_table = {}
        for tab in self.tables:
            if tab['name'] == command['name']:
                self.current_table = tab
        if self.current_table == {}:
            raise ValueError

        page1 = self.memman.get_page(0)
        table_page = mm.DataPage(page1, self.current_table['fmt'])
        data = []
        for f in self.current_table['fields']:
            if f['name'] in command['fields']:
                val = command['values'][ command['fields'].index(f['name'])]
            else:
                val = '0'
            if f['type'] == 'INT':
                val = int(val)
            elif f['type'] == 'DOUBLE':
                val = float(val)
            else:
                val = bytes(val, 'utf8')
            data.append(val)

        table_page.write((data))

    def save(self):
        with open(self.path, 'w') as db_file:
            db_file.write(str(len(self.tables))+'\n')
            for table in self.tables:
                db_file.write(table['name'] + '\n')
                db_file.write(str(len(table['fields']))+'\n')
                for field in table['fields']:
                    db_file.write(field['name']+' '+field['type'])
                    if 'size' in field.keys():
                        db_file.write(' '+ field['size'])
                    db_file.write('\n')

    def close(self):
        self.memman.close()
        self.save()
