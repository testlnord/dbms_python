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
        self.busy = True
        self._type = 0
        self.number = number

    @property
    def next_page(self):
        return struct.unpack_from('i', self._buffer[0:4])[0]

    @next_page.setter
    def next_page(self, value):
        struct.pack_into('i',self._buffer[0:4],0,value)

    @property
    def prev_page(self):
        return struct.unpack_from('i', self._buffer[4:8])[0]

    @prev_page.setter
    def prev_page(self, value):
        struct.pack_into('i',self._buffer[4:8],0,value)



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
        self.reset()

    def reset(self):
        count = Memman.page_size // self.struct_size - 1
        self.data_offset = (count//8) + 1 + 8  # 8 = page_next + page_prev
        self.bitmask = bitarray.bitarray()
        self.bitmask.frombytes(bytes(self.page._buffer[8:self.data_offset]))
        self.free_pos = 0

        self.cur_non_free = -1
        self.iterator_pos = -1
        for pos, bit in enumerate(self.bitmask):
            if bit:
                self.cur_non_free = pos


    def next_free(self):
        self.free_pos += 1
        try:
            while self.bitmask[self.free_pos]:
                self.free_pos += 1
        except IndexError:
            raise MemoryError #todo change exception
        return self.free_pos

    def __iter__(self):
        for pos, bit in enumerate(self.bitmask):
            if bit:
                self.iterator_pos = pos
                break
        return self

    def __next__(self):
        if self.iterator_pos == -1:
            raise StopIteration
        old_pos = self.iterator_pos
        self.iterator_pos = -1
        for pos, bit in enumerate(self.bitmask[old_pos+1:], start=old_pos+1):
            if bit:
                self.iterator_pos = pos
                break
        return self.read(old_pos)

    def next(self):
        if self.cur_non_free == -1:
            raise StopIteration
        old_non_free = self.cur_non_free
        self.cur_non_free = -1
        for pos, bit in enumerate(self.bitmask[old_non_free+1:], start=old_non_free+1):
            if bit:
                self.cur_non_free = pos
                break
        return self.read(old_non_free), old_non_free

    def read(self, pos):
        return struct.unpack_from(self.fmt, self.page._buffer, self.data_offset + pos*self.struct_size)

    def write(self, data, pos = None):
        if pos is None:
            pos = self.next_free()
        if Memman.page_size - (self.data_offset + pos*self.struct_size) < self.struct_size:
            raise MemoryError #todo make normal exception
        self.bitmask[pos] = True
        self.page._buffer[8:self.data_offset] = self.bitmask.tobytes()
        struct.pack_into(self.fmt, (self.page._buffer), self.data_offset + pos*self.struct_size, *data)
        self.page.clean = False
        #return pos

    def delete(self, pos):
        self.bitmask[pos] = False
        self.page._buffer[8:self.data_offset] = self.bitmask.tobytes()
        self.page.clean = False


class Memman:
    page_size = 4096
    max_pages = 20

    def __init__(self, path):
        self.pages = {}
        self.page_usages = {}

        try:
            self.file = open(path, 'r+b')

        except IOError:
            tf = open(path, 'wb')
            nothing = bytearray(Memman.page_size)
            tf.write(nothing)
            tf.close()
            self.file = open(path, 'r+b')

        self.file.seek(0, io.SEEK_END)
        self.max_pages_in_file = self.file.tell() // Memman.page_size
        self.file.seek(0, io.SEEK_SET)
        self.max_pages_in_file = 10
        self.deal_page = self.get_page(0)

    def get_page(self, page_number=None):
        if not page_number in self.pages.keys():
            if len(self.pages) >= Memman.max_pages:
                min_usages = -1
                for page in self.pages:
                    if not self.pages[page].busy:
                        if min_usages == -1:
                            min_usages = page
                            continue
                        if self.page_usages[min_usages] > self.page_usages[page]:
                            min_usages = page
                if min_usages > -1:
                    self.write_page(page)
                    del self.pages[page]
                    del self.page_usages[page]
            if page_number is None:
                page_number = self.max_pages_in_file
                self.max_pages_in_file += 1

            self.file.seek(Memman.page_size * page_number)
            buffer = bytearray(self.file.read(Memman.page_size))
            if len(buffer) < Memman.page_size:
                buffer = bytearray(Memman.page_size)
            self.pages[page_number] = Page(buffer, page_number)
            self.page_usages[page_number] = 0
        self.page_usages[page_number] += 1
        return self.pages[page_number]

    def allocate_page(self):
        return self.get_page()

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