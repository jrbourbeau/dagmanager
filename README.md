# HTCondor DAGMan management

The Directed Acyclic Graph Manager (DAGMan) is an extremely useful tool for submitting and managing a high voume of [HTCondor](https://research.cs.wisc.edu/htcondor/) jobs. However, when the number of jobs becomes large, especially when there are inter-job dependencies, actually making the DAGMan submission file can become a pain.

This project helps build and submit complex DAGMan submission files in a straight-forward manner.

## Project overview

At the end of the day, DAGMan is used to run a set of executables with various options. Additionally, there might be some inter-job dependencies you want to specify (e.g. you want job A to finish before job B begins).

So the first necessary component is the **CondorExecutable** class. This is an object that simply has two data members, a name and a path to the corresponding executable file.

The next component is the **CondorJob** class. This object encapsulates the various arguments you would like to pass to an executable and any dependencies that there might be with other jobs (e.g. parent/child relationships).

The final component is the **DagManager** class. This object is effectively just a container for the CondorJobs that are going into a DAGMan submission file.

## API overview

### `CondorExecutable`

Members:

Name | Type  |                             Description
---- | :---: | --------------------------------------:
name | `str` | Name of the `CondorExecutable` instance
path | `str` |        Path to corresponding executable

### `CondorJob`

Members:

Name             |        Type        |                                                          Description
---------------- | :----------------: | -------------------------------------------------------------------:
name             |       `str`        |                                     Name of the `CondorJob` instance
condorexecutable | `CondorExecutable` | `CondorExecutable` instance that list of arguments will be passed to
arg_list         |    `list[str]`     |                   List of arguments to be passed to condorexecutable
parent_list      | `list[CondorJob]`  |                                                  List of parent jobs
child_list       | `list[CondorJob]`  |                                                   List of child jobs

Methods:

Name         |   Argument type   |                                       Description
------------ | :---------------: | ------------------------------------------------:
add_arg      |       `str`       |                       Argument to add to arg_list
add_agrs     |      `list`       |              List of arguments to add to arg_list
add_parent   |    `CondorJob`    |          Adds `CondorJob` instance to parent_list
add_parents  | `list[CondorJob]` | Adds list of `CondorJob` instances to parent_list
add_child    |    `CondorJob`    |           Adds `CondorJob` instance to child_list
add_children | `list[CondorJob]` |  Adds list of `CondorJob` instances to child_list

#### `DagManager`

Members:

Name | Type  |   Description
---- | :---: | --------------------------------------:
name | `str` | Name of the `DagManager` instance
job_list | `list[CondorJob]` |   List of `CondorJob`s to include in DagMan submission file

Methods:

Name | Argument type  |    Description
---- | :---: | --------------------------------------:
add_job | `CondorJob` | Adds `DagManager` instance to job_list
build | `bool` | Builds all the necessary submission files. Has verbose option (`True` by default)
submit | `int` | Submits DAGMan file to Condor. Has maximum running jobs number option (default is `3000`)
build_submit | `int`, `bool` | Calls build and submit methods in sequence



## Code example

Often I find myself using DAGMan to processes several of files, say `file1.i3, file2.i3, ...` and then merge the corresponding output files into a single merged file. Schematically that might look like this:

<div style="text-align: center;">
  <img src="images/dagdiagram.png" alt="DAGMan diagram" style="width: 30%">
</div>

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
dag_manager = dagmanager.DagManager(name='process_and_merge',
                               condor_data_dir='/data/user/condor',
                               condor_scratch_dir='/scratch/user/condor')

dag_manager.add_job(process)
dag_manager.add_job(merge)
dag_manager.build_submit()
```

All the necessary submit files will be written and the DAGMan submission file will be submitted to Condor. It's that easy!

## Installation

To get dagmanager, just clone the repository via

`git clone https://github.com/jrbourbeau/dagmanager.git`

Make sure to add the path to the dagmanager repository to your system's `PYTHONPATH`.
