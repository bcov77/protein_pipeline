#!/usr/bin/env python

import os
import sys
import glob

const_data_fol = "const_data"
workers_fol = "workers"

task_list = "Task.list"
completed_list = "Completed.list"
initial_inputs_fol = "initial_inputs"
working_inputs_fol = "working_inputs"


def controller_loop():
    pipeline_file = glob.glob(os.path.join(const_data_fol, "*.pipeline"))
    assert(len(pipeline_file) == 1)

    pipeline = Pipeline(pipeline_file)

    if (not os.path.exists(workers_fol)):
        os.mkdir(workers_fol)




    current_tasks = parse_task_list(pipeline)

    # Merge output of tasks into Completed.list. Includes initial_inputs
    process_completed(pipeline, current_tasks)

    # Split inputs into command groups
    input_lists = determine_inputs(pipeline, current_tasks)

    # Find inputs that are currently being processed
    task_input_lists = get_inputs_for_tasks(pipeline, current_tasks)

    inputs_to_run_lists = []
    for i in range(len(pipeline)):
        dictt = {}
        for item in input_lists[i]:
            if item in dictt:
                print "Duplicate input: %i %s"%(i, item)
            dictt[item] = True

        for item in task_input_lists[i]:
            if item not in dictt:
                print "Missing input: %i %s"%(i, item)
            else:
                dictt[item] = False


        these_ones = []
        for key in dictt:
            if (dictt[key]):
                these_ones.append(key)
        inputs_to_run_lists.append(these_ones)


    for i in range(len(pipeline)):
        print "Stage %03i: tasked: %6i waiting: %6i"%(i, len(task_input_lists[i]), len(inputs_to_run_lists[i]))


    # Create new tasks to run
    create_new_tasks_if_possible(pipeline, current_tasks, inputs_to_run_lists)

    # Cache current tasks
    write_task_list(pipeline, current_tasks)
    current_tasks = parse_task_list(pipeline)

    running_tasknos, new_single_tasknos = schedule_tasks_to_workers(operations, current_tasks)

    known_completed_tasks, known_input_files = parse_completed_list(operations, tasks)

    print "Total tasks: %i"%len(current_tasks)
    print "Prev running tasks: %i"%(len(running_tasknos))
    print "New running tasks: %i"%(len(new_single_tasknos))
    print "Completed tasks: %i"%len(known_completed_tasks)

    

def create_new_tasks_if_possible(operations, tasks, inputs_lists):


    pipeline_empty = True
    for i in range(len(operations)):
        inputs = inputs_lists[i]

        batch_size = operations[i].batch_size

        while (len(inputs) >= batch_size or (pipeline_empty and len(inputs) > 0)):
            upper = min(batch_size, len(inputs))
            these_inputs = inputs[:upper]
            inputs = inputs[upper:]
            tasks.append(Task(operations[i], inputs, len(tasks)))
            pipeline_empty = False






def get_inputs_for_tasks(operations, tasks):

    task_input_lists = []
    for op in operations:
        task_input_lists.append([])

    for task in tasks:

        opno = task.operation.idno
        for inputt in task.inputs:
            task_input_lists[opno].append(inputt)
    return task_input_lists




def process_completed(operations, tasks):

    input_lists = []
    for i in range(len(input_lists)):
        input_lists.append([])
    input_lists.append([])

    completed_tasks, input_files = parse_completed_list(operations, tasks)

    for completed_task in completed_tasks:
        operationno = completed_task.operation.idno
        for inputt in completed_task.outputs:
            input_lists[operationno+1].append(inputt)




# handles initial inputs too
# 3 phases
#1. Identify new completed tasks, merge, and write to disk
#2. Rename original completed task files
#3. Cleanse completed list of extra filenames
def process_completed(operations, tasks):

###### 1
    known_completed_tasks, known_input_files = parse_completed_list(operations, tasks)
    new_completed_tasks, new_input_files = parse_completed_tasks(operations, tasks)
    initial_input_tasks, initial_input_files = parse_initial_inputs(operations, tasks)

    complete_dict = {}

    for task in known_completed_tasks:
        if (task.idno in complete_dict):
            print "Task already in dict?? %i"%task.idno
            continue
        complete_dict[task.idno] = task

    for task in initial_input_tasks:
        if (task.idno in complete_dict):
            print "Initial input detected as new but not new %i"%task.idno
        complete_dict[task.idno] = task

    for task in new_complete_tasks:
        if (task.idno in complete_dict):
            print "Detected as new but not new %i"%task.idno
        complete_dict[task.idno] = task

    write_complete_list(complete_dict, new_input_files + known_input_files + initial_input_files)

####### 2

    known_completed_tasks, known_input_files = parse_completed_list(operations, tasks)
    for file in known_input_files:
        if (not os.path.exists(file)):
            print "Input/output doesnt exist %s"%file
            continue
        mark_done(file)

####### 3

    final_input_files = []
    for file in known_input_files:
        if (os.path.exists(file)):
            print "Input/output exists %s"%file
            final_input_files.append(file)

    write_complete_list(complete_dict, final_input_files)


def write_complete_list(complete_dict, input_files):
    f = open_atomic(completed_list)

    for input_file in input_files:
        f.write("!%s\n"%input_file)

    for key in sorted(complete_dict.keys()):
        task = complete_dict[key]
        f.write("%08i %s"%(task.idno, "`".join(task.output)))

    close_atomic(f, complete_list)


def parse_completed_list(operations, tasks):
    f = open(completed_list)
    lines = f.readlines()
    f.close()

    completed_tasks = []
    input_files = []

    line_count = -1
    for line in lines:
        line_count += 1
        line = line.strip()
        if (len(line) == 0):
            continue
        if (line[0] == "!"):
            input_files.append(line[1:])
            continue
        sp = line.split()
        taskno = int(sp[0])
        outputs = " ".join(sp[1]).split("`")
        completed_tasks.append(tasks[taskno].make_output_task(outputs))

    return completed_tasks, input_files


def parse_completed_tasks(operations, tasks):
    worker_fols = glob.glob(os.path.join("workers_fol", "*/"))

    completed_tasks = []
    read_files = []

    for worker_fol in worker_fols:
        completeds = glob.glob(os.path.join(worker_fol, "ICompleted*.list"))
        this_final_inputs = []
        for complete in completeds:
            f = open(complete):
            lines = f.readlines()
            f.close()
            for line in lines:
                line = line.strip()
                if (len(line) == 0):
                    continue
                this_final_inputs.append(line)
            taskno = int(complete.replace("ICompleted", "").replace(".list", ""))
            completed_tasks.append(tasks[taskno].make_output_task(this_final_inputs))
            read_files.append(complete)

    return completed_tasks, read_files



def parse_initial_inputs(operations, tasks):
    files = glob.glob(os.path.join(initial_inputs_fol, "*.inputs"))

    lowest_task_idno = -1
    for task in tasks:
        lowest_task_idno = min(lowest_task_idno, task.idno)

    completed_tasks = []
    input_files = []

    idno = lowest_task_idno
    for file in files:
        idno -= 1
        completed_tasks.append(file, idno)
        input_files.append(file)

    return completed_tasks, input_files



def parse_input_list(list_file, taskno):
    f = open(list_file)
    lines = f.readlines()
    f.close()

    inputs = []
    
    line_count = -1
    for line in lines:
        line_count += 1
        line = line.strip()
        if (len(line) == 0):
            continue

        inputs.append(line)

    op = get_initial_operation()
    task = Task(op, "", taskno)
    task = task.make_output_task("`".join(inputs))

    return task




def write_task_list(operations, tasks):
    f = open_atomic(task_list)

    taskno = -1
    for task in tasks:
        taskno += 1
        assert(taskno == task.idno)
        f.write("%s %s"%(task.operation.name, "`".join(task.input)))

    close_atomic(f, task_list)


def parse_task_list(operations):
    f = open(task_list)
    lines = f.readlines()
    f.close()

    tasks = []
    initial_inputs = []

    line_count = -1
    for line in lines:
        line_count += 1
        line = line.strip()
        if (len(line) == 0):
            continue
        sp = line.split()
        inputs = " ".join(sp[1:]).split("`")
        operation_no = int(sp[0].split("_")[0])

        operation = operations[operation_no]
        tasks.append(Task(operation, inputs, line_count))

    return tasks, initial_inputs





def try_become_controller(id):






if __name__ == "__main__":
    allocation_id = sys.argv[1]
    threads = int(sys.argv[2])
    gigs = int(sys.argv[3])
    my_id = WorkerId(allocation_id, threads, gigs)
    try_become_controller(my_id)