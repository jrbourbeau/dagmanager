#!/usr/bin/env python

import os
import sys
import glob
import time
import getpass


def checkdir(outfile):
    outdir = os.path.dirname(outfile)
    if outdir == '':
        outdir = os.getcwd()
    if not os.path.isdir(outdir):
        print('\nThe directory {} doesn\'t exist...'.format(outdir)
              + 'creating it...\n')
        os.makedirs(outdir)
    return


class CondorJob(object):

    def __init__(self, name=None, executable=None):
        self.name = name
        self.executable = executable

    def __str__(self):
        output = 'CondorJob(name={}, executable={})'.format(self.name,
                                                            self.executable)
        return output


class CondorJobSeries(object):

    def __init__(self, name=None, condorjob=None):
        self.name = name
        self.condorjob = condorjob
        self.arg_list = []
        self.parent_list = []
        self.child_list = []

    def __str__(self):
        output = 'CondorJobSeries(name={}, condorjob={}, n_args={}, n_children={}, n_parents={})'.format(
            self.name, self.condorjob.name, len(self.arg_list),
            len(self.child_list), len(self.parent_list))
        return output

    def __iter__(self):
        return iter(self.arg_list)

    def add_arg(self, arg):
        self.arg_list.append(str(arg))
        return

    def add_args(self, arg_list):
        try:
            for arg in arg_list:
                self.add_arg(arg)
        except:
            raise('add_args() is expecting a list of argument strings')

        return

    def __hasparent(self, jobseries):
        return jobseries in self.parent_list

    def add_parent(self, jobseries):

        # Ensure that jobseries is a CondorJobSeries
        if not isinstance(jobseries, CondorJobSeries):
            raise TypeError('add_parent() is expecting a CondorJobSeries')

        # Don't bother continuing if jobseries is already in the parent_list
        if self.__hasparent(jobseries):
            return

        # Add job to existing parent_list
        self.parent_list.append(jobseries)
        # Add this CondorJobSeries instance as a child to the new parent
        # jobseries
        jobseries.add_child(self)

        return

    def add_parents(self, jobseries_list):

        # Ensure that jobseries_list is a list of type CondorJobSeries
        try:
            for jobseries in jobseries_list:
                self.add_parent(jobseries)
        except:
            raise('add_parents() is expecting a list of CondorJobSeries')

        return

    def __haschild(self, jobseries):
        return jobseries in self.child_list

    def add_child(self, jobseries):

        # Ensure that jobseries is a CondorJobSeries
        if not isinstance(jobseries, CondorJobSeries):
            raise TypeError('add_child() is expecting a CondorJobSeries')

        # Don't bother continuing if jobseries is already in the child_list
        if self.__haschild(jobseries):
            return

        # Add jobseries to existing child_list
        self.child_list.append(jobseries)
        # Add this CondorJobSeries instance as a parent to the new child
        # jobseries
        jobseries.add_parent(self)

        return

    def add_children(self, jobseries_list):

        # Ensure that jobseries_list is a list of type CondorJobSeries
        try:
            for jobseries in jobseries_list:
                self.add_child(jobseries)
        except:
            raise('add_children() is expecting a list of CondorJobSeries')

        return

    def haschildren(self):
        return bool(self.child_list)

    def hasparents(self):
        return bool(self.parent_list)


class DagManager(object):

    def __init__(self, name=None,
                 condor_data_dir=None, condor_scratch_dir=None):
        self.name = name
        self.condor_data_dir = condor_data_dir
        self.condor_scratch_dir = condor_scratch_dir
        self.jobseries_list = []

    def __str__(self):
        output = 'DagManager(name={}, n_jobseries={})'.format(self.name,
                                                              len(self.jobseries_list))
        return output

    def __iter__(self):
        return iter(self.jobseries_list)

    def __hasjobseries(self, jobseries):
        return jobseries in self.jobseries_list

    def add_jobseries(self, jobseries):
        # Don't bother adding jobseries if it's already in the jobseries_list
        if self.__hasjobseries(jobseries):
            return
        if isinstance(jobseries, CondorJobSeries):
            self.jobseries_list.append(jobseries)
        else:
            raise TypeError('add_job() is expecting a CondorJob')

        return

    def __get_condorjobs(self):
        job_list = [jobseries.condorjob for jobseries in self.jobseries_list]
        job_set = set(job_list)
        return job_set

    def __make_submit_script(self, job):

        # Check that paths/files exist
        if not os.path.exists(job.executable):
            raise IOError('The executable {} for CondorJob {} does not exist...'.format(
                job.executable, job.name))
        for directory in ['submit_scripts', 'logs']:
            checkdir(self.condor_scratch_dir + '/{}/'.format(directory))
        for directory in ['outs', 'errors']:
            checkdir(self.condor_data_dir + '/{}/'.format(directory))

        jobID = self.__getjobID(job)
        condor_script = self.condor_scratch_dir + \
            '/submit_scripts/{}.submit'.format(jobID)

        lines = ['universe = vanilla\n',
                 'getenv = true\n',
                 'executable = {}\n'.format(job.executable),
                 'arguments = $(ARGS)\n',
                 'log = {}/logs/{}.log\n'.format(
                     self.condor_scratch_dir, job.name),
                 'output = {}/outs/{}.out\n'.format(
                     self.condor_data_dir, job.name),
                 'error = {}/errors/{}.error\n'.format(
                     self.condor_data_dir, job.name),
                 'notification = Never\n',
                 'queue \n']

        with open(condor_script, 'w') as f:
            f.writelines(lines)

        # Add submit_file data member to job for later use
        job.submit_file = condor_script

        return

    def __getjobID(self, job):
        jobID = job.name + time.strftime('_%Y%m%d')
        othersubmits = glob.glob(
            '{}/submit_scripts/{}_??.submit'.format(self.condor_scratch_dir, jobID))
        jobID += '_{:02d}'.format(len(othersubmits) + 1)
        return jobID

    def __get_orphans(self):
        orphan_list = [
            jobseries for jobseries in self.jobseries_list if not jobseries.hasparents()]
        return orphan_list

    def build(self):
        # Get set of CondorJobs and write the corresponding submit scripts
        job_set = self.__get_condorjobs()
        for job in job_set:
            self.__make_submit_script(job)

        # Create DAG submit file path
        dagID = self.__getjobID(self)
        dag_file = '{}/submit_scripts/{}.submit'.format(
            self.condor_scratch_dir, dagID)
        self.submit_file = dag_file

        # Write dag submit file
        with open(dag_file, 'w') as dag:
            for jobseries in self:
                for i, arg in enumerate(jobseries):
                    dag.write('JOB {}_p{} '.format(jobseries.name, i) +
                              jobseries.condorjob.submit_file + '\n')
                    dag.write('VARS {}_p{} '.format(
                        jobseries.name, i) + 'ARGS="' + arg + '"\n')
                # Add parent/child information if necessary
                if jobseries.hasparents():
                    parent_string = 'Parent'
                    for parentseries in jobseries.parent_list:
                        for j, parentarg in enumerate(parentseries):
                            parent_string += ' {}_p{}'.format(
                                parentseries.name, j)
                    child_string = 'Child'
                    for k, arg in enumerate(jobseries):
                        child_string += ' {}_p{}'.format(jobseries.name, k)
                    dag.write(parent_string + ' ' + child_string + '\n')

        return

    def submit(self, maxjobs=3000):
        os.system(
            'condor_submit_dag -maxjobs {} {}'.format(maxjobs, self.submit_file))
        return

    def build_submit(self, maxjobs=3000):
        self.build()
        self.submit(maxjobs)
        return
