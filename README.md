# HTCondor DAGMan management

The Directed Acyclic Graph Manager (DAGMan) is an extremely useful tool for submitting and managing a high voume of HTCondor jobs. One simply needs to create a DAG submission file and submit it to Condor.

However, when the number of jobs becomes large, especially when there are inter-job dependencies, actually making a DAG submission file can become a pain. This project is an attempt to create complex DAG submission files in a straight-forward manner.

## Core components

At the end of the day, DAGMan is used to run a set of executables with various options. In addition, there might even be some inter-job dependencies (e.g. you want job A to finish before job B begins).

So the first component is the `CondorExecutable` class. This is an object that simply has two members, a name and a path to the executable file.

The next component is the `CondorJob` class. This is an object that encapsulates the various arguments you would like to pass to an executable and any dependencies that there might be with other jobs (e.g. parent/child relationships).

The final component is the `DagManager` class. This is effectively just a container for the CondorJobs that are going into a DAG submission file.


## API example

Often I find myself using DAGMan to processes a bunch of files, say `file1.i3, file2.i3, ...` and then merge the outputs into a single file. Schematically that might look like this:

![DAGMan diagram](images/dagdiagram.png "DAGMan diagram")

Below is a a quick example of how to implement the above process using dagmanager.

```python
import dagmanager

# Specify the executables that will be run
process_ex = dagmanager.CondorExecutable(name='process', path='/path/to/process.py')
merge_ex = dagmanager.CondorExecutable(name='merge', path='/path/to/merge.py')

# Specify the CondorJobs arguments and any dependencies
process = dagmanager.CondorJob(name='process', condorexecutable=process_ex)
process.add_arg('--input file1.i3 --output outputfile1.hdf5')
process.add_arg('--input file2.i3 --output outputfile2.hdf5')
merge = dagmanager.CondorJob(name='merge', condorexecutable=merge_ex)
merge.add_arg('--overwrite')
# Make sure process job completes before merge begins
merge.add_parent(process)

# Finally create a DagManager, add all the CondorJobs, build DAGMan submission file and submit!
dagmanager = dagmanager.DagManager(name='process_and_merge',
                               condor_data_dir='/data/user/condor',
                               condor_scratch_dir='/scratch/user/condor')

dagmanager.add_job(process)
dagmanager.add_job(merge)
dagmanager.build_submit()

```

All the necessary submit files will be written and the DAGMan submission file will be submitted to Condor. It's that easy!
