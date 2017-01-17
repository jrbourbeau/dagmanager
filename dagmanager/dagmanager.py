#!/usr/bin/env python

import os
import glob
import time
from . import logger


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

    def __init__(self, name, path, request_memory=None,
                 request_disk=None, queue=None, lines=None, verbose=0):

        self.name = str(name)
        self.path = str(path)
        self.request_memory = request_memory
        self.request_disk = request_disk
        self.queue = queue
        if lines is None:
            lines = []
        if isinstance(lines, str):
            lines = [lines]
        self.lines = lines

        # Set up logger
        self.logger = logger.setup_logger(self, verbose)

    def __repr__(self):
        output = 'CondorExecutable(name={}, path={}, request_memory={}, request_disk={}, n_lines={})'.format(
            self.name, self.path, self.request_memory, self.request_disk, len(self.lines))
        return output

    def add_line(self, line):
        self.lines.append(str(line))
        self.logger.debug(
            'Added \'{}\' to lines for CondorExecutable {}'.format(str(line), self.name))
        return

    def add_lines(self, lines):
        if isinstance(lines, str):
            lines = [lines]
        try:
            for line in lines:
                self.add_line(line)
        except:
            raise TypeError('add_lines() is expecting a list of strings')

        return


class CondorJob(object):

    def __init__(self, name, condorexecutable=None, verbose=0):

        self.name = str(name)
        self.condorexecutable = condorexecutable
        self.args = []
        self.parents = []
        self.children = []

        # Set up logger
        self.logger = logger.setup_logger(self, verbose)

    def __repr__(self):
        output = 'CondorJob(name={}, condorexecutable={}, n_args={}, n_children={}, n_parents={})'.format(
            self.name, self.condorexecutable.name, len(self.args),
            len(self.children), len(self.parents))
        return output

    def __iter__(self):
        return iter(self.args)

    def add_arg(self, arg):
        self.args.append(str(arg))
        self.logger.debug(
            'Added \'{}\' to args for CondorJob {}'.format(str(arg), self.name))
        return

    def add_args(self, args):
        try:
            for arg in args:
                self.add_arg(arg)
        except:
            raise TypeError(
                'add_args() is expecting a list of argument strings')

        return

    def _hasparent(self, job):
        return job in self.parents

    def add_parent(self, job):

        # Ensure that job is a CondorJob
        if not isinstance(job, CondorJob):
            raise TypeError('add_parent() is expecting a CondorJob')

        # Don't bother continuing if job is already in the parents
        if self._hasparent(job):
            return

        # Add job to existing parents
        self.parents.append(job)
        self.logger.debug(
            'Added {} to parents for CondorJob {}'.format(job.name, self.name))
        # Add this CondorJob instance as a child to the new parent job
        job.add_child(self)

        return

    def add_parents(self, job_list):

        # Ensure that job_list is a list of type CondorJob
        try:
            for job in job_list:
                self.add_parent(job)
        except:
            raise TypeError('add_parents() is expecting a list of CondorJobs')

        return

    def _haschild(self, job):
        return job in self.children

    def add_child(self, job):

        # Ensure that job is a CondorJob
        if not isinstance(job, CondorJob):
            raise TypeError('add_child() is expecting a CondorJob')

        # Don't bother continuing if job is already in the children
        if self._haschild(job):
            return

        # Add job to existing children
        self.children.append(job)
        self.logger.debug(
            'Added {} to children for CondorJob {}'.format(job.name, self.name))
        # Add this CondorJob instance as a parent to the new child job
        job.add_parent(self)

        return

    def add_children(self, job_list):

        # Ensure that job_list is a list of type CondorJob
        try:
            for job in job_list:
                self.add_child(job)
        except:
            raise TypeError('add_children() is expecting a list of CondorJobs')

        return

    def haschildren(self):
        return bool(self.children)

    def hasparents(self):
        return bool(self.parents)


class DagManager(object):

    def __init__(self, name,
                 condor_data_dir=None, condor_scratch_dir=None, verbose=0):

        self.name = str(name)
        self.condor_data_dir = condor_data_dir
        self.condor_scratch_dir = condor_scratch_dir
        self.jobs = []

        # Set up logger
        self.logger = logger.setup_logger(self, verbose)

    def __repr__(self):
        output = 'DagManager(name={}, n_jobs={})'.format(self.name,
                                                         len(self.jobs))
        return output

    def __iter__(self):
        return iter(self.jobs)

    def _hasjob(self, job):
        return job in self.jobs

    def add_job(self, job):
        # Don't bother adding job if it's already in the jobs list
        if self._hasjob(job):
            return
        if isinstance(job, CondorJob):
            self.jobs.append(job)
        else:
            raise TypeError('add_job() is expecting a CondorJob')
        self.logger.debug(
            'Added {} to jobs for DagManager {}'.format(job.name, self.name))

        return

    def _get_executables(self):
        executable_list = [job.condorexecutable for job in self.jobs]
        executable_set = set(executable_list)
        return executable_set

    def _make_submit_script(self, executable):

        # Check that paths/files exist
        if not os.path.exists(executable.path):
            raise IOError(
                'The path {} does not exist...'.format(executable.path))
        for directory in ['submit_scripts', 'logs']:
            checkdir(self.condor_scratch_dir + '/{}/'.format(directory))
        for directory in ['outs', 'errors']:
            checkdir(self.condor_data_dir + '/{}/'.format(directory))

        jobID = self._getjobID(executable)
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

        # Re-format lines if queue option specified
        if executable.queue:
            if not isinstance(executable.queue, int):
                raise TypeError('The queue option for CondorExecutable {} is {}, expecting an int'.format(
                    executable.name, executable.queue))
            lines[-1] = 'queue {}\n'.format(executable.queue)
            lines[4:7] = ['log = {}/logs/{}_$(Process).log\n'.format(self.condor_scratch_dir, jobID),
                          'output = {}/outs/{}_$(Process).out\n'.format(
                              self.condor_data_dir, jobID),
                          'error = {}/errors/{}_$(Process).error\n'.format(self.condor_data_dir, jobID)]

        # Add memory and disk requests, if specified
        if executable.request_memory:
            lines.insert(-2, 'request_memory = {}\n'.format(executable.request_memory))
        if executable.request_disk:
            lines.insert(-2, 'request_disk = {}\n'.format(executable.request_disk))

        # Add any extra lines to submit file, if specified
        if executable.lines:
            if isinstance(executable.lines, str):
                lines.insert(-2, executable.lines + '\n')
            elif isinstance(executable.lines, list):
                for line in executable.lines:
                    lines.insert(-2, line + '\n')
            else:
                raise TypeError('The lines option for CondorExecutable {} is of type {}, expecting str or list'.format(
                    executable.name, type(executable.lines)))

        with open(condor_script, 'w') as f:
            f.writelines(lines)

        # Add submit_file data member to job for later use
        executable.submit_file = condor_script

        return

    def _getjobID(self, executable):
        jobID = executable.name + time.strftime('_%Y%m%d')
        othersubmits = glob.glob(
            '{}/submit_scripts/{}_??.submit'.format(self.condor_scratch_dir, jobID))
        jobID += '_{:02d}'.format(len(othersubmits) + 1)
        return jobID

    def build(self):
        # Get set of CondorExecutable and write the corresponding submit
        # scripts
        executable_set = self._get_executables()
        for executable in executable_set:
            self._make_submit_script(executable)

        # Create DAG submit file path
        dagID = self._getjobID(self)
        dag_file = '{}/submit_scripts/{}.submit'.format(
            self.condor_scratch_dir, dagID)
        self.submit_file = dag_file

        # Write dag submit file
        self.logger.info(
            'Building DAG submission file {}...'.format(self.submit_file))
        with open(dag_file, 'w') as dag:
            for job_index, job in enumerate(self, start=1):
                self.logger.info('Working on CondorJob {} [{} of {}]'.format(
                    job.name, job_index, len(self.jobs)))
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
                            parent_string += ' {}_p{}'.format(
                                parentjob.name, j)
                    child_string = 'Child'
                    for k, arg in enumerate(job):
                        child_string += ' {}_p{}'.format(job.name, k)
                    dag.write(parent_string + ' ' + child_string + '\n')

        self.logger.info('DAG submission file successfully built!')

        return

    def submit(self, maxjobs=3000, **kwargs):
        command = 'condor_submit_dag -maxjobs {} {}'.format(
            maxjobs, self.submit_file)
        for option, value in kwargs.iteritems():
            command += ' {} {}'.format(option, value)
        os.system(command)
        return

    def build_submit(self, maxjobs=3000, **kwargs):
        self.build()
        self.submit(maxjobs, **kwargs)
        return
