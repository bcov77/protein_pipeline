#!/usr/bin/env python

import os
import sys
import glob
import time



def make_temp_filename(filename):
    return filename + ".tmp"

def open_atomic(filename):
    tmp = make_temp_filename(filename)
    return open(tmp, "w")

def close_atomic(handle, filename):
    handle.close()
    tmp = make_temp_filename(filename)
    os.rename(tmp, filename)

def check_failed_atomic(filename):
    return os.path.exists(make_temp_filename(filename))

def mark_done(filename):
    os.path.rename(filename, filename + ".done")

def cmd(command, wait=True):
    # print ""
    # print command
    the_command = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if (not wait):
        return
    the_stuff = the_command.communicate()
    return the_stuff[0] + the_stuff[1]

def age_of_file_minutes(filename):
    return (time.time() - os.path.getmtime(filename)) / 60




