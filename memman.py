#! /usr/bin/env python
# coding=utf-8

import struct

class Page:
    def __init__(self, buffer):
        self._buffer = buffer
        self.clean = True
        self._busy = True
        self._type = 0
        self._fmt = ''

    def getBuffer(self):
        raise NotImplemented

class ListPage(Page):
    pass

class DataPage(Page):
    def __init__(self, fmt):
        pass


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
            buffer = self.file.read(Memman.page_size)
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
            self.file.write(self.pages[page_number].getBuffer())

    def close(self):
        for page in self.pages:
            self.write_page(page)
        self.file.close()