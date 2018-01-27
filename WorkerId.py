#!/usr/bin/env python

import json



class WorkerId:

    allocation_id = ""
    allocation_cpus = 0
    allocation_gigs = 0

    my_id = ""
    my_cpus = 0
    my_gigs = 0

    def __init__(self, allocation_id, cpus, gigs):
        self.allocation_id = allocation_id
        self.allocation_cpus = cpus
        self.allocation_gigs = gigs

    def from_string(self, string):
        js = json.loads(string)
        self.allocation_id = js["allocation_id"]
        self.allocation_cpus = js["allocation_cpus"]
        self.allocation_gigs = js["allocation_gigs"]

        self.my_id = js["my_id"]
        self.my_cpus = js["my_cpus"]
        self.my_gigs = js["my_gigs"]

    def to_string(self):
        dictt = {
            "allocation_id":self.allocation_id
            "allocation_cpus":self.allocation_cpus
            "allocation_gigs":self.allocation_gigs

            "my_id":self.my_id
            "my_cpus":self.my_cpus
            "my_gigs":self.my_gigs
        }
        return json.dumps(dictt)


def kill_worker(worker_id):


def split_worker(worker_id):







def combine_worker(worker_ids):

