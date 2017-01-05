#!/usr/bin/env python

import os
import argparse

import dagmanager

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Prints vegetables of your choosing')
    parser.add_argument('--length', dest='length',
                   default=10,
                   help='Number of random numbers to be written to file')
    args = parser.parse_args()

    # Create CondorExecutable for example_script.py
    ex = dagmanager.CondorExecutable(name='save_numbers_script', path='{}/write_num_to_file.py'.format(os.getcwd()))
    # Create CondorJob that will pass arguments to example_script.py
    job = dagmanager.CondorJob(name='job', condorexecutable=ex)
    job.add_arg('--length {}'.format(args.length))
    # Create DagManager to build and submit jobs
    manager = dagmanager.DagManager(name='manager',
        condor_data_dir='{}/example_condor_dir'.format(os.getcwd()),
        condor_scratch_dir='{}/example_condor_dir'.format(os.getcwd()))
    manager.add_job(job)
    manager.build_submit()
