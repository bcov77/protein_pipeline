#!/usr/bin/env python

import os
import sys
import glob


class Task:

    operation = ""
    inputs = []
    idno = -100000000000000

    outputs = []

    def __init__(self, operation, inputs, idno):
        self.operation = operation
        self.inputs = inputs
        self.idno = idno

    def make_output_task(self, outputs):
        to_ret = Task(self.operation, self.inputs, self.idno)
        to_ret.outputs = outputs
        return to_ret
