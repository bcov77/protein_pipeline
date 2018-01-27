#!/usr/bin/env python

import os
import sys
import re


def get_initial_operation():
    op = Operation()
    op.initial_operation = True
    op.idno = -1
    return op

class Operation:

    name = ""
    batch_size = 0
    executable = ""
    cpu_size = 0
    max_minutes = 0
    idno = 0

    initial_operation = False


    def __init__(self):
        pass


    def __init__(self, lines, ordinal):
        self.parse_lines(lines, ordinal)
        self.sanity_check()

    def parse_lines(self, lines, ordinal):
        header = lines[0].strip().replace(":", "").replace(" ", "_")
        self.name = "%03i_%s"%(ordinal, header)
        self.idno = ordinal

        for line in lines[1:]:
            line = line.strip()
            sp = line.split(":")

            key = sp[0].lower()
            arg = sp[1].strip()

            if (key == "batchsize"):
                self.batch_size = int(arg)
            if (key == "executable"):
                self.executable = arg
            if (key == "cpurequest"):
                self.cpu_size = int(arg)
            if (key == "maxminutes"):
                self.max_minutes = int(arg)



    def sanity_check(self):
        if (self.initial_operation):
            return
        assert(self.batch_size > 0)
        assert(os.path.exists(self.executable))
        assert(self.cpu_size > 0)
        assert(self.max_minutes > 0)


class Pipeline:

    operations = []

    def __init__(self, filename):
        self.parse_file(filename)

    def parse_file(self, filename):
        f = open(filename)
        lines = f.readlines()
        f.close()

        for line in lines:
            line = re.sub("#.*", "", line)

        operation_lines = []
        for line in lines + ["asdf"]:
            if (len(line.strip()) == 0):
                continue
            if (line[0] != " " and line[0] != "\t"):
                if (len(operation_lines) > 0):
                    self.operations.append(Operation(operation_lines, len(self.operations)))
                operation_lines = []

            operation_lines.append(line)







