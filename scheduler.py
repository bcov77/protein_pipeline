#!/usr/bin/env python

import os
import sys
import glob
from collections import defaultdict

from controller import *


dead_extra_mins = 10


#order tasks from furthest in pipeline to least
#Order cpus from least free cores to most free
#Go down tasks and try to accommodate
#Split workers if needed, and combine workers if needed
#Quit when a task can't be started even if other tasks later in the queue could be

def schedule_tasks_to_workers(operations, tasks):
    worker_fols = glob.glob(os.path.join("workers_fol", "*/"))

    ready_workers, running_tasknos = find_ready_workers_and_running_tasks(operations, tasks)

    to_run = order_waiting_tasks_by_priority(operations, tasks, running_tasknos)

    single_cpus = []
    grouped_singles = []    # these are whole nodes sitting idle that have been split
    multi_cpus = []

    for worker_id in ready_workers:
        if (worker_id.my_cpus == 1):
            single_cpus.append(worker_id)
        else:
            multi_cpus.append(worker_id)

    grouped = defaultdict(lambda : [], {})

    for worker_id in single_cpus:
        grouped[worker_id.allocation_id].append(worker_id)

    delete_single_ids = []
    for allocation_id in grouped:
        example = grouped[allocatin_id][0]
        if (example.allocation_cpus == len(grouped[allocation_id])):
            grouped_singles.append(grouped[allocation_id])
            delete_single_ids.append(allocation_id)

    temp_single_cpus = single_cpus
    single_cpus = []
    for single_cpu in temp_single_cpus:
        if (single_cpu.allocation_id in delete_single_ids):
            continue
        single_cpus.append(single_cpu)

    single_cpus = sorted(single_cpus, lambda x: len(grouped[x.allocation_id]))

    new_running_tasknos = try_to_schedule(to_run, single_cpus, grouped_singles, multi_cpus)

    total_cpus = 0
    for allocation_id in grouped:
        total_cpus += len(grouped[allocation_id])

    print "Total cpus: %i"%total_cpus


    return running_tasknos, new_running_tasknos


def try_to_schedule(to_run, single_cpus, grouped_singles, multi_cpus):
    single_cpu_next = 0
    grouped_single_next = 0
    multi_cpu_next = 0
    to_run_next = 0

    new_running_tasknos = []

    while (to_run_next < len(to_run)):
        this_to_run = to_run[to_run_next]

        if (this_to_run.operation.cpu_size == 1):
            if (single_cpu_next == len(single_cpus)):
                if (grouped_single_next == len(grouped_singles)):
                    if (multi_cpu_next == len(multi_cpus)):
                        break   # this means we're out of cpus
                    single_cpus += split_worker(multi_cpus[multi_cpu_next])
                    multi_cpu_next += 1
                else:
                    single_cpus += grouped_singles[grouped_single_next]
                    grouped_single_next += 1


            assert(single_cpu_next != len(single_cpus))
            schedule(this_to_run, single_cpus[single_cpu_next])
            single_cpu_next += 1
            new_running_tasknos.append(this_to_run.idno)
        else:
            if (multi_cpu_next == len(multi_cpus)):
                if (grouped_single_next == len(grouped_singles)):
                    break  # we may not be out be we can't do much else
                multi_cpus += combine_worker(grouped_singles[grouped_single_next])
                grouped_single_next += 1

            assert(multi_cpu_next != len(multi_cpus))
            schedule(this_to_run, multi_cpus[multi_cpu_next])
            multi_cpu_next += 1
            new_running_tasknos.append(this_to_run.idno)


        to_run_next += 1


    print "Scheduled tasks that couldn't be run: %i"%(len(to_run) - to_run_next)

    print "Idle fragmented cpus: %s"%(len(single_cpus) - single_cpu_next)
    print "Idle full nodes: %s"%(
        len(grouped_singles) + len(multi_cpus) - grouped_single_next - multi_cpu_next)

    if (to_run_next == len(to_run)):
        for multi_cpu in multi_cpus[multi_cpu_next:]:
            kill_worker(multi_cpu)
        for grouped_single in grouped_singles[grouped_single_next:]:
            for ind in grouped_single:
                kill_worker(ind)

    return new_running_tasknos
            



# higher command.idno gets priority to make the pipeline flow
def order_waiting_tasks_by_priority(operations, tasks, running_tasknos):

    known_completed_tasks, known_input_files = parse_completed_list(operations, tasks)

    task_should_run = {}
    for task in tasks:
        task_should_run[task.idno] = True
    for task in known_completed_tasks:
        if (task.idno not in task_should_run):
            print "Task not found %i"%task.idno
        else:
            task_should_run[task.idno] = False
    for idno in running_tasknos:
        if (idno not in task_should_run):
            print "Runing but not found %i"%idno
        else:
            task_should_run[idno] = False

    to_run = []
    for taskno in task_should_run:
        if (task_should_run[taskno]):
            to_run.append(tasks[taskno])

    to_run = sorted(to_run, key=lambda x : -x.command.idno)

    return to_run



def find_ready_workers_and_running_tasks(operations, tasks):
    worker_fols = glob.glob(os.path.join("workers_fol", "*/"))

    ready_workers = []
    running_tasknos = []

    for fol in worker_fols:
        alive, running_taskno, waiting = determine_is_alive(fol)
        if (not alive):
            continue
        if (running_taskno >= 0):
            running_tasknos.append(running_taskno)

        if (not waiting):
            continue

        f = open(os.path.join(fol, "info"))
        worker_id = WorkerId(f.read())
        f.close()

        ready_workers.append(worker_id)
        

    return ready_workers, running_tasknos



# alive, running task no, waiting
def determine_is_alive(operations, tasks, fol):
    files = list_files_new_to_old(fol)

    #first look for known dead
    for file in files:
        if (file == "IDead"):
            return False, -1

    #next look for waiting
    for file in files:
        if (file == "IWaiting"):
            age = age_of_file_minutes(os.path.join(fol, file))
            if (age < dead_extra_mins):
                return True, -1, True
            break

    #next look for running
    for file in files:
        if (re.match("IAccept[0-9]+", file) != None):
            age = age_of_file_minutes(os.path.join(fol, file))
            taskno = int(re.match("IAccept([0-9]+)", file).group(1))
            task_length = tasks[taskno].operation.max_minutes

            if (age < task_length + dead_extra_mins):
                return True, taskno, False
            break

    #next look for recent input assignment
    for file in files:
        if (re.match("Inputs[0-9]+", file) != None):
            age = age_of_file_minutes(os.path.join(fol, file))
            taskno = int(re.match("Inputs([0-9]+)", file).group(1))

            if (age < dead_extra_mins):
                return True, taskno, False
            break

    print "Not sure if dead or alive: %s: %s"%(fol, " ".join(files))

    return True, -1, False


def schedule(task, worker_id):
    fol = worker_id.get_my_fol()

    input_name = "Input%08i"%task.idno

    open_atomic(input_name)
    f.write("%s\n"%task.executable)
    for inputt in task.inputs:
        f.write("%s\n"%inputt)
    close_atomic(f, input_name)









