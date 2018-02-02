#!/usr/bin/env python

import os
import sys
import glob
import time
import re

from WorkerId_ import *
from util import *
from Task_ import *


workers_fol = "workers"




# check for IDead
# check for Inputs


def worker_loop(worker_id):

    my_fol = worker_id.get_my_fol()

    while (True):

        if (os.path.exists("all_done")):
            print "Looks like all done"
            cmd("echo a > %s"%os.path.join(my_fol, "IDead"))
            sys.exit(0)

        if (age_of_file_minutes("last_schedule") > 60):
            print "Looks like controller died"
            cmd("echo a > %s"%os.path.join(my_fol, "IDead"))
            sys.exit(0)


        files = list_files_new_to_old(my_fol)

        if ("IDead" in files):
            worker_death_handler(worker_id, files)

        for file in files:
            if (re.match("Inputs[0-9]+$", file) != None):
                worker_run_job(worker_id, file)


        # although there are no inputs yet, inputs may have been
        #  written to the file system already. This is handled in
        #  scheduler.py

        cmd("echo a >> %s"%os.path.join(my_fol, "IWaiting"))

        time.sleep(15)






def worker_death_handler(worker_id, files):
    if ("ISplit" in files):
        worker_split_handler(worker_id)

    time.sleep(15)
    # This would get called when the original worker revives
    my_fol = worker_id.get_my_fol()
    files = list_files_new_to_old(my_fol)

    if ("IMerged" in files):
        worker_merge_handler(worker_id)

    sys.exit(0)



def worker_run_job(worker_id, file):
    taskno = int(re.match("Inputs([0-9]+)$", file).group(1))

    my_fol = worker_id.get_my_fol()

    input_file = os.path.join(my_fol, file)

    f = open(input_file)
    lines = f.readlines()
    f.close()

    mark_done(input_file)

    executable = lines[0].strip()

    inputs = []
    for line in lines[1:]:
        line = line.strip()
        if (len(line) == 0):
            continue
        inputs.append(line)

    iaccept = os.path.join(my_fol, "IAccept%08i"%taskno)
    icompleted = os.path.join(my_fol, "ICompleted%08i.list"%taskno)
    log_file = os.path.join(my_fol, "log%08i.log"%taskno)

    print "Running job %08i"%taskno

    the_command = "%s %s %s > %s 2>&1"%(executable, icompleted, " ".join(inputs), log_file)
    print the_command

    cmd("echo a > %s"%iaccept)
    cmd(the_command)

    print "Done"
    if (os.path.exists(os.path.join(my_fol, "IDead"))):
        print "Got killed while running a job!!!!!! WTF!!!!! %08i"%taskno

    #ensure controller knows we're done before we ask for another
    time.sleep(30)


def worker_split_handler(worker_id):
    my_fol = worker_id.get_my_fol()

    worker_executable = os.path.join(get_script_dir(), "worker.py")

    isplit = os.path.join(my_fol, "ISplit")


    print "Splitting into individual threads"

    print cmd("cat %s | parallel '%s %s %i {} '"%(isplit, worker_executable,
        worker_id.allocation_id, worker_id.allocation_cpus))




class MergeException(Exception):
    pass

def worker_merge_handler(worker_id):
    print "Individual threads merge into one"
    raise MergeException()











if __name__ == "__main__":
    allocation_id = sys.argv[1]
    threads = int(sys.argv[2])
    extras = ""
    if (len(sys.argv) > 3):
        extras = sys.argv[3]
    

    my_id = WorkerId(allocation_id, threads, extras)

    new_worker = True
    while (new_worker):
        new_worker = False

        new_worker_setup(my_id)

        try:
            worker_loop(my_id)
        except MergeException as e:
            my_id = WorkerId(my_id.allocation_id + "__", threads)
            new_worker = True









