Kadoop
======

Instructions for use:

* enter the source directory: > cd src
* if desired, edit config.json
* build the project: > make all
* change your permissions to permit execution of the startup and shutdown scripts: > chmod u+x start.sh; chmod u+x shutdown.sh
* start up the Distributed File System and Map Reduce framework: > ./start.sh 
* press return if necessary, then run Kadoop: > java MR/Kadoop
