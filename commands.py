#! /usr/bin/env python
# coding=utf-8


class CreateTable():
    def __init__(self, db):
        self.name = ''
        self.fields = []
        self.db = db

    def run(self):
        self.db.add({'name': self.name, 'fields': self.fields})


class Insert():
    def __init__(self, db):
        self.name = ''
        self.fields = []
        self.values = []
        self.db = db

    def run(self):
        print ("insert into "+self.name)
        print ("FIELDS")
        for field in self.fields:
            print (field)
        print ("VALUES")
        for val in self.values:
            print (val)
