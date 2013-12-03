Kadoop
======

Kadoop is a map-reduce framework with custom distributed file system.

## Setup

* Enter the source directory: 
  > cd src
  
* If desired, modify the settings by editing config.json

* Build the project: 
  > make all
  
* Change your permissions to permit execution of the startup and shutdown scripts: 
  > chmod u+x start.sh; chmod u+x shutdown.sh
  
* Start up the Distributed File System and Map Reduce framework: 
  > ./start.sh 
  
* Press return if necessary, then run Kadoop: 
  > java MR/Kadoop [args]

## Examples

* java MR/Kadoop data.txt WordCount_Mapper WordCount_Reducer 3

## Cleaning Up

* Once you are done (or if you wish to restart), you should execute the shutdown script:
  > ./shutdown.sh 

* That's it! 
