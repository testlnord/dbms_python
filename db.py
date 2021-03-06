#! /usr/bin/env python
# coding=utf-8



import memman as mm
import struct
import itertools


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
        try:
            with open(path,'r'):
               pass
        except IOError:
            with open(path,'w') as f:
                f.write("0\n")

        with open(path,'r') as db_file:
            table_count = int(db_file.readline())
            for ti in range(table_count):
                table = {}
                table['name'] = db_file.readline().strip()
                table['start'] = int(db_file.readline().strip())
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
        page_free = self.memman.allocate_page()
        table['start'] = page_free.number
        page_busy = self.memman.allocate_page()
        page_data = self.memman.allocate_page()
        page_free = mm.DataPage(page_free, 'I')
        page_free.write([page_data.number])
        page_busy.clean = False
        page_data.clean = False
        table['fmt'] = db._make_fmt(table['fields'])
        self.tables.append(table)
        print("Table %s created." %table['name'])
        page_busy.busy = False
        page_free.page.busy = False
        page_data.busy = False

    def safe_write(self, page, data, pos=None):
        try:
            page.write(data, pos)
            return True
        except MemoryError:
            new_page = self.memman.allocate_page()
            new_page.next_page = page.page.next_page
            new_page.prev_page = page.page.number
            page.page.next_page = new_page.number
            page.page.busy = False
            page.page = new_page
            page.reset()
            page.write(data, pos)
            return False



    def insert(self, command):
        self.current_table = {}
        for tab in self.tables:
            if tab['name'] == command['name']:
                self.current_table = tab
        if self.current_table == {}:
            #todo make my own exception and avoid crashes
            raise ValueError

        page_free = self.memman.get_page(self.current_table['start'])
        #page_busy = self.memman.get_page(self.current_table['start'] + 1)
        page_free = mm.DataPage(page_free, 'I')
        page_num, free_record_num = page_free.next()
        page1 = self.memman.get_page(page_num[0])
        table_page = mm.DataPage(page1, self.current_table['fmt'])
        data = []
        for f in self.current_table['fields']:
            if f['name'] in command['fields']:
                val = command['values'][command['fields'].index(f['name'])]
            else:
                val = '0'
            if f['type'] == 'INT':
                val = int(val)
            elif f['type'] == 'DOUBLE':
                val = float(val)
            else:
                val = bytes(val, 'utf8')
            data.append(val)
        old_page_num = table_page.page.number
        if not self.safe_write(table_page,data):
            page_busy = self.memman.get_page(self.current_table['start'] + 1)
            page_busy = mm.DataPage(page_busy, 'I')
            page_free.delete(free_record_num)
            self.safe_write(page_busy,[old_page_num])
            self.safe_write(page_free,[table_page.page.number])
            page_busy.busy = False

        table_page.page.busy = False
        page_free.page.busy = False

        print("1 row(s) affected.") #todo write proper output

    def get_next_page(self, page):
        if page.page.next_page != 0:
            page.page.busy = False
            page.page = self.memman.get_page(page.page.next_page)
            page.reset()
        else:
            raise IndexError
        return page

    def delete(self, table_name):
        pass

    def scan_table(self, table_name):
        self.current_table = {}
        for tab in self.tables:
            if tab['name'] == table_name:
                self.current_table = tab
        if self.current_table == {}:
            raise ValueError

        out_page = mm.DataPage(self.memman.allocate_page(), self.current_table['fmt'])
        start_page_number = out_page.page.number

        page_free = self.memman.get_page(self.current_table['start'])
        page_busy = self.memman.get_page(self.current_table['start']+1)
        page_free = mm.DataPage(page_free, 'I')
        page_busy = mm.DataPage(page_busy, 'I')
        while True:
            for page_num in page_free:
                page = self.memman.get_page(page_num[0])
                page = mm.DataPage(page, self.current_table['fmt'])
                try:
                    for val in page:
                        self.safe_write(out_page, val)
                        #print(val)
                        #todo add printing in right format
                except struct.error:
                    pass
                page.page.busy = False
            if page_free.page.next_page == 0:
                break
            else:
                new_page = self.memman.get_page(page_free.page.next_page)
                page_free.page.busy = False
                page_free.page = new_page
                page_free.reset()
        page_free.page.busy = False

        while True:
            for page_num in page_busy:
                #if page_num[0] > 200:
                #    print ("======================\n  PANIC %s PANIC \n===================="%(page_num[0]))
                #    continue#break
                page = self.memman.get_page(page_num[0])
                page = mm.DataPage(page, self.current_table['fmt'])
                try:
                    for val in page:
                        self.safe_write(out_page, val)
                        #print(val)
                except struct.error:
                    pass

            if page_busy.page.next_page == 0:
                break
            else:
                new_page = mm.DataPage(db.memman.get_page(page_busy.page.next_page),'I')
                page_busy.page.busy = False
                page_busy.page = new_page
                page_busy.reset()
        page_busy.page.busy = False
        out_page.page.busy = False
        return start_page_number, self.current_table['fmt']
    #todo proj
    def projection(self,page_it, mask):
        pass
    #todo join
    def join(self, page1_it, page2_it, field1, field2):
        pass

    def page_iter(self,fmt, first_page_number, last_page_number=None, delete=False):
        page  = mm.DataPage(self.memman.get_page(first_page_number), fmt)
        yield page
        while page.page.next_page != 0:
            if page.page.next_page == last_page_number:
                break
            page.page.busy = False
            if delete:
                page.page.temp = True
            page.page = self.memman.get_page(page.page.next_page)
            yield page
        if delete:
            page.page.temp = True
        page.page.busy = False

    def sort(self, page_number, fmt, field):
        page = mm.DataPage(self.memman.get_page(page_number), fmt)
        pages = [page]
        max_pages_for_sorting = mm.Memman.max_pages - 5
        page_nums = [page_number]
        one_pass = True
        out_page = mm.DataPage(self.memman.allocate_page(), fmt)
        first_out_page_number = out_page.page.number
        step = 1
        first_page = True
        while page.page.next_page != 0 or first_page:
            first_page = False
            for i in range(1, max_pages_for_sorting):
                try:
                    pages.append(self.get_next_page(page))
                    page_nums.append(pages[-1].page.number)
                except IndexError:
                    break
            if page.page.next_page != 0:
                one_pass = False
            for rec in sorted(itertools.chain(*pages), key=lambda rec: rec[field]):
                self.safe_write(out_page, rec)
            for page in pages:
                page.page.busy = False
            pages = []
        out_page.busy = False
        return first_out_page_number
        """
        out_page.page.busy = False
        while not one_pass:
            one_pass = True
            step += max_pages_for_sorting - 1
            out_page = mm.DataPage(self.memman.get_page(first_out_page_number))
            first_out_page_new = out_page.page.number
            pages = [out_page]
            for p in range(0,len(page_nums),step):
                pages.append(mm.DataPage(self.get_next_page(out_page)))

            for rec in sorted(itertools.chain(*pages), key=lambda rec: rec[field]):
                self.safe_write()
        """
    #todo finish sort
    #todo sort join

    def print(self, page_num, fmt):
        for page in self.page_iter(fmt, page_num, delete=True):
            for rec in page:
                for el in rec:
                    if hasattr(el,'decode'):
                        el = el.strip(b'\x00').decode('utf-8')

                    print(str(el),end='\t')
                print()


    def select(self, table_name, fields):
        p, f =  self.scan_table(table_name)
        p = self.sort(p, f, 0)
        self.print(p, f)
        """
        self.current_table = {}
        for tab in self.tables:
            if tab['name'] == table_name:
                self.current_table = tab
        if self.current_table == {}:
            raise ValueError

        page_free = self.memman.get_page(self.current_table['start'])
        page_busy = self.memman.get_page(self.current_table['start']+1)
        page_free = mm.DataPage(page_free, 'I')
        page_busy = mm.DataPage(page_busy, 'I')
        while True:
            for page_num in page_free:
                page = self.memman.get_page(page_num[0])
                page = mm.DataPage(page, self.current_table['fmt'])
                try:
                    for val in page:
                        print(val)
                        #todo add printing in right format
                except struct.error:
                    pass
                page.page.busy = False
            if page_free.page.next_page == 0:
                break
            else:
                new_page = self.memman.get_page(page_free.page.next_page)
                page_free.page.busy = False
                page_free.page = new_page
                page_free.reset()
        page_free.page.busy = False

        while True:
            for page_num in page_busy:
                #if page_num[0] > 200:
                #    print ("======================\n  PANIC %s PANIC \n===================="%(page_num[0]))
                #    continue#break
                page = self.memman.get_page(page_num[0])
                page = mm.DataPage(page, self.current_table['fmt'])
                try:
                    for val in page:
                        print(val)
                except struct.error:
                    pass

            if page_busy.page.next_page == 0:
                break
            else:
                new_page = mm.DataPage(db.memman.get_page(page_busy.page.next_page),'I')
                page_busy.page.busy = False
                page_busy.page = new_page
                page_busy.reset()
        page_busy.page.busy = False
        """

    #todo delete
    #todo select syntax
    #todo delete syntax
    #todo update
    #todo update syntax
    #todo magic iterator throw data
    #todo B-tree OPTIONAL

    def save(self):
        with open(self.path, 'w') as db_file:
            db_file.write(str(len(self.tables))+'\n')
            for table in self.tables:
                db_file.write(table['name'] + '\n')
                db_file.write(str(table['start']) + '\n')
                db_file.write(str(len(table['fields']))+'\n')
                for field in table['fields']:
                    db_file.write(field['name']+' '+field['type'])
                    if 'size' in field.keys():
                        db_file.write(' '+ field['size'])
                    db_file.write('\n')

    def close(self):
        self.memman.close()
        self.save()
