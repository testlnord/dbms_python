#! /usr/bin/env python
# coding=utf-8

import struct
import bitarray
from ctypes import c_char

class Page:
    def __init__(self, buffer):
        self._buffer = buffer
        self.clean = True
        self._busy = True
        self._type = 0

    def get_buffer(self):
        return self._buffer


class ListPage(Page):
    def __init__(self, buffer):
        super(ListPage,self).__init__(buffer)
        self._fmt = 'i'
        
    pass

class DataPage():
    def __init__(self, page, fmt):
        self.fmt = fmt
        self.page = page
        self.struct_size = struct.calcsize(fmt)
        count = Memman.page_size // self.struct_size - 1
        self.data_offset = count//8+1;
        self.bitmask = bitarray.bitarray(self.page._buffer[0:self.data_offset])
        self.free_pos = 0

    def next_free(self):
        while self.bitmask[self.free_pos]:
            self.free_pos += 1
        return self.free_pos

    def write(self, data, pos = None):
        if pos is None:
            pos = self.next_free()
        self.bitmask[pos] = True
        struct.pack_into(self.fmt, (self.page._buffer), self.data_offset + pos*self.struct_size,*data)
        self.page.clean = False

#(c_char * Memman.page_size).from_buffer
class Memman:
    page_size = 4048
    max_pages = 20

    def __init__(self, path):
        self.pages = {}
        try:
            self.file = open(path, 'r+b')

        except IOError:
            tf = open(path, 'w')
            tf.close()
            self.file = open(path, 'r+b')

    def get_page(self, page_number):
        if not page_number in self.pages.keys():
            if len(self.pages) == Memman.max_pages:
                for page in self.pages:
                    if not self.pages[page].busy:
                        self.write_page(page)
                        del self.pages[page]
                        break

            self.file.seek(Memman.page_size * page_number)
            #buffer = (self.file.read(Memman.page_size))

            buffer = (bytearray(Memman.page_size))
            if len(buffer) == 0:
                buffer = bytearray(Memman.page_size)
            self.pages[page_number] = Page(buffer)
        return self.pages[page_number]

    def allocate_page(self):
        buffer = bytearray(Memman.page_size)
        return Page(buffer)

    def deallocate_page(self, page):
        pass

    def write_page(self, page_number):
        if not self.pages[page_number].clean:
            self.file.seek(Memman.page_size * page_number)
            self.file.write(self.pages[page_number].get_buffer())

    def close(self):
        for page in self.pages:
            self.write_page(page)
        self.file.close()