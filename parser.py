import commands
import sys

class Words:
    class Reader:
        def __init__(self):
            self._c = sys.stdin.read(1)
        def c(self):
            return self._c
        def n(self):
            self._c = sys.stdin.read(1)

    def __init__(self):
        self.rdr = self.Reader()
        self.run = True
        self._cur = ''

    def cur(self):
        return  self._cur

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if not self.run:
            raise StopIteration()
        state = 0
        word = ''
        while True:
            if state == 0:
                c = self.rdr.c()
                if c.isalpha() or c == '_':
                    state = 1  # word
                elif c.isdigit():
                    state = 2  # number
                elif c in ',*;:\".)(=':
                    word = c
                    self.rdr.n()
                    break
                else:
                    self.rdr.n()
            elif state == 1:  # word
                while self.rdr.c().isalnum() or self.rdr.c() == '_':
                    word += self.rdr.c()
                    self.rdr.n()
                break

            elif state == 2:  # number
                dot_f = False
                while self.rdr.c().isdigit() or self.rdr.c() == '.':
                    if c == '.' and not dot_f:
                        dot_f = True
                    if c == '.' and dot_f:
                        break
                    word += self.rdr.c()
                    self.rdr.n()
                break


        self._cur = word
        return word


class Parser:
    def __init__(self, db):
        self.iter = Words()
        self.db = db

    def next(self):
        command = None
        command = self.query()
        return command
    
    def into_q(self, command):
        if self.iter.next().upper() == 'INTO':
            pass
        else:
            raise SyntaxError
        command.name = self.iter.next()
        if self.iter.next() == '(':
            pass
        else:
            raise SyntaxError
        while self.iter.next() != ')':
            if self.iter.cur() == ',':
                continue
            command.fields.append(self.iter.cur())


    def values_q(self, command):
        if self.iter.next().upper() == 'VALUES':
            pass
        else:
            raise SyntaxError
        if self.iter.next() == '(':
            pass
        else:
            raise SyntaxError
        while self.iter.next() != ')':
            if self.iter.cur() == ',':
                continue
            command.values.append(self.iter.cur())


    def insert_q(self, command):
        self.into_q(command)
        self.values_q( command)


    def fields_q(self, command):
        if self.iter.next()== '*':
            command.fields = ['*']
        else:
            while self.iter.cur().upper() != 'FROM':
                if self.iter.cur() != ',':
                    command.fields.append(self.iter.cur())
                self.iter.next()


    def from_q(self, command):
        if self.iter.cur().upper() != 'FROM':
            raise SyntaxError
        command.name = self.iter.next()
        self.iter.next()

    def set_q(self, command):
        if not self.iter.next().upper() == 'SET':
            raise SyntaxError
        comma = True
        while True:
            if comma:
                command.fields.append(self.iter.next())
                if self.iter.next() != '=':
                    raise SyntaxError
                command.values.append(self.iter.next())
                comma = False
            else:
                if self.iter.next() != ',':
                    break
                else:
                    comma = True

    def where_q(self, command):
        if not self.iter.cur().upper() != 'WHERE':
            return


    def select_q(self, command):
        self.fields_q( command)
        if not len(command.fields):
            raise SyntaxError
        self.iter.next()
        self.from_q( command)
        self.where_q(command)

    def delete_q(self, command):
        self.iter.next()
        self.from_q(command)
        self.where_q(command)

    def update_q(self, command):
        command.name = self.iter.next()
        self.set_q(command)
        self.where_q(command)

    def tab_field_q(self, command):
        fld = {}
        if self.iter.cur().upper() in ['INT', 'FLOAT', 'VARCHAR']:
            fld['type'] = self.iter.cur().upper()
        if self.iter.next() == '(':
            fld['size'] = self.iter.next()
            if self.iter.next() != ')':
                raise SyntaxError
            self.iter.next()
        fld['name'] = self.iter.cur()
        command.fields.append(fld)


    def crt_table_q(self, command):
        command.name = self.iter.next()
        if self.iter.next() == '(':
            pass
        else:
            raise SyntaxError
        while self.iter.next() != ')':
            if self.iter.cur() == ',':
                continue
            self.tab_field_q(command)


    def query(self):
        lex = self.iter.next().upper()
        if lex == 'INSERT':
            command = commands.Insert(self.db)
            self.insert_q( command)
            return command
        elif lex == 'SELECT':
            command = commands.Select(self.db)
            self.select_q(command)
            return  command
        elif lex == 'DELETE':
            command = commands.Delete(self.db)
            self.delete_q(command)
            return command
        elif lex ==  'UPDATE':
            command = commands.Update(self.db)
            self.update_q(command)
            return command
        elif lex == 'CREATE':
            lex = self.iter.next().upper()
            if lex == 'TABLE':
                command  = commands.CreateTable(self.db)
                self.crt_table_q( command)
                return command
        elif lex == 'EXIT' or lex == 'CLOSE':
            self.iter.run = False
            return None
        elif lex == ';':
            return None

