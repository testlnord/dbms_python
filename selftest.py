import sys
import re
import commands
import db
import os
import random


try:
    os.remove("test.txt")
    os.remove("test.txtdb")
except:
    pass
db = db.db("test.txt")

com = commands.CreateTable(db)
com.name = 'TEST'
com.fields = [{'type': 'INT', 'name': 'D1'}, {'type': 'VARCHAR', 'name': 'SS', 'size': '20'}]
com.run()

for i in range(1000):
    com = commands.Insert( db)
    com.name = 'TEST'
    com.fields = ['D1',  'SS']
    com.values = [random.randint(1,1000), 'ASDFGAGadsfasdfddef23fef']
    com.run()

com = commands.Select(db)
com.name = 'TEST'
com.fields = ['*']
com.run()
try:
    os.remove("test.txt")
    os.remove("test.txtdb")
except:
    pass