#! /usr/bin/env python
# coding=utf-8

import struct
import bitarray
from ctypes import c_char
import io


class Page:
    def __init__(self, buffer, number):
        self._buffer = buffer
        self.clean = True
        self._busy = True
        self._type = 0
        self.number = number

    def get_buffer(self):
        return self._buffer


class ListPage(Page):
    def __init__(self, page):
        self._fmt = 'i'
        self.page = page
        self.struct_size = struct.calcsize(self._fmt)
        count = Memman.page_size // self.struct_size - 1
        self.data_offset = count//8+1
        self.bitmask = bitarray.bitarray(self.page._buffer[0:self.data_offset])
        self.free_pos = 0

        
    pass

class DataPage():
    def __init__(self, page, fmt):
        self.fmt = fmt
        self.page = page
        self.struct_size = struct.calcsize(fmt)
        count = Memman.page_size // self.struct_size - 1
        self.data_offset = count//8+1;
        self.bitmask = bitarray.bitarray()
        self.bitmask.frombytes(self.page._buffer[0:self.data_offset])
        self.free_pos = 0

        self.cur_non_free = -1
        for pos, bit in enumerate(self.bitmask):
            if bit:
                self.cur_non_free = pos


    def next_free(self):
        while self.bitmask[self.free_pos]:
            self.free_pos += 1
        return self.free_pos

    def __iter__(self):
        return self

    def __next__(self):
        if self.cur_non_free == -1:
            raise StopIteration
        return self.next()

    def next(self):
        if self.cur_non_free == -1:
            raise StopIteration
        old_non_free = self.cur_non_free
        self.cur_non_free = -1
        for pos, bit in enumerate(self.bitmask[old_non_free+1:]):
            if bit:
                self.cur_non_free = pos
        return self.read(old_non_free)

    def read(self, pos):
        return struct.unpack_from(self.fmt, self.page._buffer, self.data_offset + pos*self.struct_size)

    def write(self, data, pos = None):
        if pos is None:
            pos = self.next_free()
        self.bitmask[pos] = True
        self.page._buffer[0:self.data_offset] = self.bitmask.tobytes()
        struct.pack_into(self.fmt, (self.page._buffer), self.data_offset + pos*self.struct_size,*data)
        self.page.clean = False

#(c_char * Memman.page_size).from_buffer
class Memman:
    page_size = 4096
    max_pages = 20

    def __init__(self, path):
        self.pages = {}

        try:
            self.file = open(path, 'r+b')

        except IOError:
            tf = open(path, 'w')
            tf.close()
            self.file = open(path, 'r+b')

        self.file.seek(0, io.SEEK_END)
        self.max_pages_in_file = self.file.tell() // Memman.page_size
        self.file.seek(0, io.SEEK_SET)
        self.deal_page = self.get_page(0)

    def get_page(self, page_number):
        if not page_number in self.pages.keys():
            if len(self.pages) == Memman.max_pages:
                #todo lru
                for page in self.pages:
                    if not self.pages[page].busy:
                        self.write_page(page)
                        del self.pages[page]
                        break

            self.file.seek(Memman.page_size * page_number)
            buffer = (self.file.read(Memman.page_size))

            #buffer = (bytearray(Memman.page_size))
            if len(buffer) == 0:
                buffer = bytearray(Memman.page_size)
            self.pages[page_number] = Page(buffer, page_number)
        return self.pages[page_number]

    def allocate_page(self):
        #todo: add checking for max_page_count
        buffer = bytearray(Memman.page_size)
        page_num = self.max_pages_in_file
        self.max_pages_in_file += 1
        self.pages[page_num] = Page(buffer, page_num)
        return self.pages[page_num]

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