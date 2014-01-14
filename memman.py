#! /usr/bin/env python
# coding=utf-8

import struct
import bitarray
import io


class Page:
    def __init__(self, buffer, number):
        self._buffer = buffer
        self.clean = True
        self.busy = True
        self._type = 0
        self.number = number
        self.temp = False
        self.pointer_fmt = struct.Struct('i')
    @property
    def next_page(self):
        return self.pointer_fmt.unpack_from( self._buffer[0:4])[0]

    @next_page.setter
    def next_page(self, value):
        self.pointer_fmt.pack_into(self._buffer[0:4],0,value)

    @property
    def prev_page(self):
        return self.pointer_fmt.unpack_from( self._buffer[4:8])[0]

    @prev_page.setter
    def prev_page(self, value):
        self.pointer_fmt.pack_into( self._buffer[4:8],0,value)



    def get_buffer(self):
        return self._buffer


class ListPage(Page):
    def __init__(self, page):
        raise DeprecationWarning
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
        self.fmt = struct.Struct(fmt)
        self.page = page
        self.reset()

    def reset(self):
        count = Memman.page_size // self.fmt.size - 1
        self.data_offset = (count//8) + 1 + 8  # 8 = page_next + page_prev
        self.bitmask = bitarray.bitarray()
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

    #todo check for cur_non_free correct values
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
        return self.fmt.unpack_from(self.page._buffer, self.data_offset + pos*self.fmt.size)

    def write_all(self, data):
        for pos, record in enumerate(data):
            self.write(record, pos)

    def write(self, data, pos=None):
        if pos is None:
            pos = self.next_free()
        if Memman.page_size - (self.data_offset + pos*self.fmt.size) < self.fmt.size:
            raise MemoryError #todo make normal exception
        self.bitmask[pos] = True
        self.page._buffer[8:self.data_offset] = self.bitmask.tobytes()
        self.fmt.pack_into( (self.page._buffer), self.data_offset + pos*self.fmt.size, *data)
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
        self.deallocate_pages_list = DataPage(self.get_page(0), 'I')

    def get_page(self, page_number=None):
        if not page_number in self.pages.keys():
            if len(self.pages) >= Memman.max_pages:
                min_usages = -1
                for page in self.pages:
                    if not self.pages[page].busy:
                        if self.pages[page].temp:
                            self.deallocate_page(self.pages[page])
                            min_usages = 3
                            break
                        if min_usages == -1:
                            min_usages = page
                            continue
                        if self.page_usages[min_usages] > self.page_usages[page]:
                            min_usages = page
                if min_usages > -1:
                    if self.pages[min_usages].temp :
                        self.deallocate_page(min_usages)
                    else:
                        self.write_page(min_usages)
                    del self.pages[min_usages]
                    del self.page_usages[min_usages]
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
        try:
            page_num, pos = self.deallocate_pages_list.next()
            self.deallocate_pages_list.delete(pos)
            return self.get_page(page_num)
        except StopIteration:
            return self.get_page()

    def allocate_temp_page(self):
        page = self.allocate_page()
        page.temp = True
        return page

    def deallocate_page(self, page_number):
        try:
            self.deallocate_pages_list.write(page_number)
            return True
        except MemoryError:
            new_page = self.memman.allocate_page()
            new_page.next_page = self.deallocate_pages_list.page.next_page
            new_page.prev_page = self.deallocate_pages_list.page.number
            self.deallocate_pages_list.page.next_page = new_page.number
            self.deallocate_pages_list.page.busy = False
            self.deallocate_pages_list.page = new_page
            self.deallocate_pages_list.reset()
            self.deallocate_pages_list.write(page_number)


    def write_page(self, page_number):
        if not self.pages[page_number].clean:
            self.file.seek(Memman.page_size * page_number)
            self.file.write(self.pages[page_number].get_buffer())


    def close(self):
        for page in self.pages:
            self.write_page(page)
        self.file.close()