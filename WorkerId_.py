#!/usr/bin/env python

import json
import os
import sys

from util import *

workers_fol = "workers"

class WorkerId:

    allocation_id = ""
    allocation_cpus = 0

    my_id = ""
    my_cpus = 0

    def __init__(self, allocation_id, cpus, extra=""):
        self.allocation_id = allocation_id
        self.allocation_cpus = cpus

        if (extra == ""):
            self.my_id = self.allocation_id
            self.my_cpus = self.allocation_cpus
        else:
            self.my_id = self.allocation_id + extra
            self.my_cpus = 1

    def from_string(self, string):
        js = json.loads(string)
        self.allocation_id = js["allocation_id"]
        self.allocation_cpus = js["allocation_cpus"]

        self.my_id = js["my_id"]
        self.my_cpus = js["my_cpus"]

    def to_string(self):
        print self.allocation_cpus, "!!@!$@#$@#$@"
        dictt = {
            "allocation_id":self.allocation_id,
            "allocation_cpus":self.allocation_cpus,

            "my_id":self.my_id,
            "my_cpus":self.my_cpus
        }
        return json.dumps(dictt)

    def get_my_fol(self):
        return os.path.join(workers_fol, self.my_id)

    def get_allocation_fol(self):
        return os.path.join(workers_fol, self.my_id)

    def make_my_fol(self):
        fol = self.get_my_fol()
        if (not os.path.exists(fol)):
            try:
                os.mkdir(fol)
            except:
                pass
            info = os.path.join(fol, "info")
            f = open_atomic(info)
            f.write(self.to_string())
            close_atomic(f, info)
        else:
            print "My folder already exists!! " + fol

def worker_id_from_string(string):
    wid = WorkerId("", 0)
    wid.from_string(string)
    return wid


def kill_worker(worker_id):
    my_fol = worker_id.get_my_fol()
    cmd("echo a > %s"%os.path.join(my_fol, "IDead"))


def split_worker(worker_id):
    my_fol = worker_id.get_my_fol()

    isplit = os.path.join(my_fol, "ISplit")

    print "Splitting: %s %i"%(worker_id.allocation_id, worker_id.allocation_cpus)

    new_ids = []

    f = open_atomic(isplit)
    for i in range(worker_id.allocation_cpus):
        extra = "_%02i"%i
        f.write(extra + "\n")
        new_id = WorkerId(worker_id.allocation_id, worker_id.allocation_cpus, extra)
        new_id.make_my_fol()
        new_ids.append(new_id)
    close_atomic(f, isplit)

    kill_worker(worker_id)


    return new_ids


def combine_worker(worker_ids):
    my_fol = worker_id.get_my_fol()

    allocation_id = worker_ids[0].allocation_id
    allocation_fol = worker_ids[0].get_allocation_fol

    print "Combining workers: %s"%allocation_id

    for worker_id in worker_ids:
        if (allocation_id != worker_id.allocation_id):
            print "Merging different allocations!!!! %s %s"%(
                allocation_id, worker_id.allocation_id)
        print worker_id.my_id


    cmd("echo a > %s"%os.path.join(allocation_fol, "IMerged"))

    for worker_id in worker_ids:
        kill_worker(worker_id)

    new_id = WorkerId(allocation_id, worker_ids[0].allocation_cpus)
    new_id.make_my_fol()






def new_worker_setup(worker_id):
    worker_id.make_my_fol()
    my_fol = worker_id.get_my_fol()
    print ""
    print ""
    print "New worker at: %s"%my_fol
    print "With %i cpus"%worker_id.my_cpus
    print "Parent id: %s"%worker_id.allocation_id
    print "Parent cpus: %s"%worker_id.allocation_cpus

















