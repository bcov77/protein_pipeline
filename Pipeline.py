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
    mem_size = 0
    max_minutes = 0
    idno = 0

    initial_operation = False


    def __init__(self):
        pass


    def __init__(self, lines, ordinal):
        parse_lines(lines, ordinal)
        sanity_check()

    def parse_lines(lines, ordinal):
        header = lines[0].strip().replace(":", "").replace(" ", "_")
        name = "%03i_%s"%(ordinal, header)
        idno = ordinal

        for line in lines[1:]:
            line = lines.strip()
            sp = line.split(":")

            key = sp.lower()
            arg = sp[1]

            if (key == "batchsize"):
                batch_size = int(arg)
            if (key == "executable"):
                executable = arg
            if (key == "cpurequest"):
                cpu_size = int(arg)
            if (key == "memrequest"):
                mem_size = int(arg)
            if (key == "maxminutes"):
                max_minutes = int(arg)



    def sanity_check():
        if (initial_operation):
            return
        assert(batch_size > 0)
        assert(os.path.exists(executable))
        assert(cpu_size > 0)
        assert(mem_size > 0)
        assert(max_minutes > 0)


class Pipeline:

    operations = []

    def __init__(self, filename):
        parse_file(filename)

    def parse_file(filename):
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
                    operations.append(Operation(operation_lines), len(operations))
                operation_lines = []

            operation_lines.append(line)
