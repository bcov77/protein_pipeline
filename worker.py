#!/usr/bin/env python

import os
import sys
import glob
import time

workers_fol = "workers"




# check for IDead
# check for Inputs


def worker_loop(worker_id):

    my_fol = worker_id.get_my_fol()

    while (True):

        files = list_files_new_to_old(my_fol)

        if ("IDead" in files):
            worker_death_handler(worker_id, files)

        for file in files:
            if (re.match("Inputs[0-9]+", file) != None):
                worker_run_job(worker_id, file)

        cmd("echo a >> %s"%os.path.join(my_fol, "IWaiting"))

        time.sleep(60)






def worker_death_handler(worker_id, files):
    if ("ISplit" in files):
        worker_split_handler(worker_id)

    # This would get called when the original worker revives
    if ("IMerged" in files):
        worker_merge_handler(worker_id)

    sys.exit(0)



def worker_run_job(worker_id, file):
    taskno = int(re.match("Inputs([0-9]+)", file).group(1))

    my_fol = worker_id.get_my_fol()


    f = open(file)
    lines = f.readlines()
    f.close()

    executable = lines[0].strip()

    inputs = []
    for line in lines[1:]:
        line = line.strip()
        if (len(line) == 0):
            continue
        inputs.append(line)

    iaccept = os.path.join(my_fol, "IAccept%08i")
    icompleted = os.path.join(my_fol, "ICompleted%08i"%taskno)
    log_file = os.path.join(my_fol, "log%08i.log"%taskno)

    cmd("echo a > %s"%iaccept)
    cmd("%s %s %s > %s 2>&1"%(executable, icompleted, " ".join(inputs), log_file))



def worker_split_handler(worker_id):
    my_fol = worker_id.get_my_fol()

    worker_executable = os.path.join(get_script_dir(), "worker.py")

    isplit = os.path.join(my_fol, "ISplit")


    print "Splitting into individual threads"

    print cmd("cat %s | parallel '%s %s %i {}'"%(isplit, worker_executable,
        worker_id.allocation_id, 1))




class MergeException(Exception):
    pass

def worker_merge_handler(worker_id):
    print "Individual threads merge into one"
    except MergeException()











if __name__ == "__main__":
    allocation_id = int(sys.argv[1])
    threads = int(sys.argv[2])
    
    new_worker = True
    while (new_worker):
        new_worker = False

        my_id = WorkerId(allocation_id, threads)
        new_worker_setup(my_id)

        try:
            worker_loop(my_id)
        except MergeException as e:
            my_id = WorkerId(my_id.allocation_id + "__", threads)
            new_worker = True









