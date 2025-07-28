# coding: utf-8
from __future__ import division, unicode_literals

from .algorithm import algorithm_generate
from .method import method
from .resources import alphabet, size


def generate(alphabet=alphabet, size=size):
    return method(algorithm_generate, alphabet, size)
