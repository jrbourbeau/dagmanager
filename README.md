# HTCondor DAGMan management

![HTCondor logo](images/HTCondor_red_blk.png "HTCondor logo")


The Directed Acyclic Graph Manager (DAGMan) is an extremely useful tool for submitting and managing a high voume of HTCondor jobs. One simply needs to create a DAG submission file and submit it to Condor.

However, when the number of jobs becomes large, especially when there are inter-job dependencies, actually making a DAG submission file can become a pain. This project is an attempt to create complex DAG submission files in a straight-forward manner.

## Core components

At the end of the day, DAGMan is used to run a set of executables with various options. In addition, there might even be some inter-job dependencies (e.g. you want job A to finish before job B begins).

So the first component is the `CondorJob` class. This is an object that simply has two members, a name and a path to the job executable file.

![CondorJob layout](images/condorjob.pdf "CondorJob layout")

The next component is the `CondorJobSeries` class. This is an object that encapsulates the various arguments you would like to pass to an executable and any dependencies that there might be with other jobs.

The final component is the `DagManager` class. This is effectively just a container for the `CondorJobSeries` that are going into a DAG submission file.
