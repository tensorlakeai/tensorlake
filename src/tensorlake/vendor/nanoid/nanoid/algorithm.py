from __future__ import division, unicode_literals

from os import urandom


def algorithm_generate(random_bytes):
    return bytearray(urandom(random_bytes))
