# Goal
The goal is to check and compare performance of different approaches for data loading in Oracle databases.
The tested approaches are:
* classic approach using intermediate tables for inter-process data transfer and communication
* approach with single process running it all and utilizing pipelined functions
* approach with multiple processes communication with Oracle Advanced Queueing
