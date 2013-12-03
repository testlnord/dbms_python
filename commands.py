#! /usr/bin/env python
# coding=utf-8


class CreateTable():
    def __init__(self, db):
        self.name = ''
        self.fields = []
        self.db = db

    def run(self):
        self.db.add({'name': self.name, 'fields': self.fields})


class Select():
    def __init__(self, db):
        self.db = db
        self.name = ''
        self.fields = []

    def run(self):
        self.db.select(self.name, self.fields)


class Insert():
    def __init__(self, db):
        self.name = ''
        self.fields = []
        self.values = []
        self.db = db

    def run(self):
        command = {'fields': [j for x in self.fields for j in x.values()], 'values': self.values, 'name': self.name}
        self.db.insert(command)