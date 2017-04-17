import logging
from pybloom import BloomFilter
from pybloom import ScalableBloomFilter


class BloomFilter:

    def __init__(self, capacity=None, error_rate=0.001,
                 mode=ScalableBloomFilter.LARGE_SET_GROWTH):

        self.capacity = capacity

        if capacity is None:
            self.bf = ScalableBloomFilter(mode=mode)
        else:
            self.bf = BloomFilter(capacity=capacity, error_rate=error_rate)

    def add_one(self, entry):
        self.bf.add(entry)

    def add_many(self, entries):
        for no, entry in enumerate(entries):
            if no % 2000 == 0:
                logging.debug('{} entries added!')
            self.add_one(entry)

    def lookup(self, item):
        return item in self.bf

if __name__ == '__main__':
    bf = BloomFilter()
    id = '928325ceda23825627d10edd4fac3f01'
    bf.add_one(id)
