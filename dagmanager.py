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


class CondorExecutable(object):

    def __init__(self, name=None, path=None):
        self.name = name
        self.path = path

    def __str__(self):
        output = 'CondorExecutable(name={}, path={})'.format(
            self.name, self.path)
        return output


class CondorJob(object):

    def __init__(self, name=None, condorexecutable=None):
        self.name = name
        self.condorexecutable = condorexecutable
        self.args = []
        self.parents = []
        self.children = []

    def __str__(self):
        output = 'CondorJob(name={}, condorexecutable={}, n_args={}, n_children={}, n_parents={})'.format(
            self.name, self.condorexecutable.name, len(self.args),
            len(self.children), len(self.parents))
        return output

    def __iter__(self):
        return iter(self.args)

    def add_arg(self, arg):
        self.args.append(str(arg))
        return

    def add_args(self, args):
        try:
            for arg in args:
                self.add_arg(arg)
        except:
            raise('add_args() is expecting a list of argument strings')

        return

    def __hasparent(self, job):
        return job in self.parents

    def add_parent(self, job):

        # Ensure that job is a CondorJob
        if not isinstance(job, CondorJob):
            raise TypeError('add_parent() is expecting a CondorJob')

        # Don't bother continuing if job is already in the parents
        if self.__hasparent(job):
            return

        # Add job to existing parents
        self.parents.append(job)
        # Add this CondorJob instance as a child to the new parent job
        job.add_child(self)

        return

    def add_parents(self, job_list):

        # Ensure that job_list is a list of type CondorJob
        try:
            for job in job_list:
                self.add_parent(job)
        except:
            raise('add_parents() is expecting a list of CondorJobs')

        return

    def __haschild(self, job):
        return job in self.children

    def add_child(self, job):

        # Ensure that job is a CondorJob
        if not isinstance(job, CondorJob):
            raise TypeError('add_child() is expecting a CondorJob')

        # Don't bother continuing if job is already in the children
        if self.__haschild(job):
            return

        # Add job to existing children
        self.children.append(job)
        # Add this CondorJob instance as a parent to the new child job
        job.add_parent(self)

        return

    def add_children(self, job_list):

        # Ensure that job_list is a list of type CondorJob
        try:
            for job in job_list:
                self.add_child(job)
        except:
            raise('add_children() is expecting a list of CondorJobs')

        return

    def haschildren(self):
        return bool(self.children)

    def hasparents(self):
        return bool(self.parents)


class DagManager(object):

    def __init__(self, name=None,
                 condor_data_dir=None, condor_scratch_dir=None):
        self.name = name
        self.condor_data_dir = condor_data_dir
        self.condor_scratch_dir = condor_scratch_dir
        self.jobs = []

    def __str__(self):
        output = 'DagManager(name={}, n_jobs={})'.format(self.name,
                                                         len(self.jobs))
        return output

    def __iter__(self):
        return iter(self.jobs)

    def __hasjob(self, job):
        return job in self.jobs

    def add_job(self, job):
        # Don't bother adding job if it's already in the jobs list
        if self.__hasjob(job):
            return
        if isinstance(job, CondorJob):
            self.jobs.append(job)
        else:
            raise TypeError('add_job() is expecting a CondorJob')

        return

    def __get_executables(self):
        executable_list = [job.condorexecutable for job in self.jobs]
        executable_set = set(executable_list)
        return executable_set

    def __make_submit_script(self, executable):

        # Check that paths/files exist
        if not os.path.exists(executable.path):
            raise IOError('The path {} does not exist...'.format(executable.path))
        for directory in ['submit_scripts', 'logs']:
            checkdir(self.condor_scratch_dir + '/{}/'.format(directory))
        for directory in ['outs', 'errors']:
            checkdir(self.condor_data_dir + '/{}/'.format(directory))

        jobID = self.__getjobID(executable)
        condor_script = self.condor_scratch_dir + \
            '/submit_scripts/{}.submit'.format(jobID)

        lines = ['universe = vanilla\n',
                 'getenv = true\n',
                 'executable = {}\n'.format(executable.path),
                 'arguments = $(ARGS)\n',
                 'log = {}/logs/{}.log\n'.format(
                     self.condor_scratch_dir, jobID),
                 'output = {}/outs/{}.out\n'.format(
                     self.condor_data_dir, jobID),
                 'error = {}/errors/{}.error\n'.format(
                     self.condor_data_dir, jobID),
                 'notification = Never\n',
                 'queue \n']

        with open(condor_script, 'w') as f:
            f.writelines(lines)

        # Add submit_file data member to job for later use
        executable.submit_file = condor_script

        return

    def __getjobID(self, executable):
        jobID = executable.name + time.strftime('_%Y%m%d')
        othersubmits = glob.glob(
            '{}/submit_scripts/{}_??.submit'.format(self.condor_scratch_dir, jobID))
        jobID += '_{:02d}'.format(len(othersubmits) + 1)
        return jobID

    def build(self, verbose=True):
        # Get set of CondorExecutable and write the corresponding submit scripts
        executable_set = self.__get_executables()
        for executable in executable_set:
            self.__make_submit_script(executable)

        # Create DAG submit file path
        dagID = self.__getjobID(self)
        dag_file = '{}/submit_scripts/{}.submit'.format(
            self.condor_scratch_dir, dagID)
        self.submit_file = dag_file

        # Write dag submit file
        if verbose:
            print('Building DAG submission file {}...'.format(self.submit_file))
        with open(dag_file, 'w') as dag:
            for job_index, job in enumerate(self):
                if verbose:
                    print('\tWorking on CondorJob {} [{} of {}]'.format(
                        job.name, job_index + 1, len(self.jobs)))
                for i, arg in enumerate(job):
                    dag.write('JOB {}_p{} '.format(job.name, i) +
                              job.condorexecutable.submit_file + '\n')
                    dag.write('VARS {}_p{} '.format(
                        job.name, i) + 'ARGS="' + arg + '"\n')
                # Add parent/child information if necessary
                if job.hasparents():
                    parent_string = 'Parent'
                    for parentjob in job.parents:
                        for j, parentarg in enumerate(parentjob):
                            parent_string += ' {}_p{}'.format(parentjob.name, j)
                    child_string = 'Child'
                    for k, arg in enumerate(job):
                        child_string += ' {}_p{}'.format(job.name, k)
                    dag.write(parent_string + ' ' + child_string + '\n')
        if verbose:
            print('DAG submission file successfully built!')

        return

    def submit(self, maxjobs=3000, **kwargs):
        command = 'condor_submit_dag -maxjobs {} {}'.format(maxjobs, self.submit_file)
        for option, value in kwargs.iteritems():
            command += ' {} {}'.format(option, value)
        os.system(command)
        return

    def build_submit(self, maxjobs=3000, verbose=True, **kwargs):
        self.build(verbose)
        self.submit(maxjobs, **kwargs)
        return
