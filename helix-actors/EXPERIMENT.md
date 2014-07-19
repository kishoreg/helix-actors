Experiment
==========

This document describes an experiment by which the performance of Netty-based
Helix Actors can be evaluated.

Overview
--------

A Helix cluster with two participants will be set up. These participants will
send messages to each other without any throttling, as fast as they can. We
will monitor the following metrics:

1. Number of messages per second
2. Number of bytes written to network per second
3. Number of errors (i.e. write to channel failed)
4. Number of times a channel to recipient was opened

Persistent channels are used, so a high value for (4) indicates that the
networking layer is behaving poorly.

Resources
---------

Assuming that we have gigabit ethernet switches connecting these nodes. So
saturated network means gigabit of data sent.

### Participants (2)

(eat1-app210.stg.linkedin.com, eat1-app211.stg.linkedin.com)

* 24-core Intel(R) Xeon(R) CPU X5650  @ 2.67GHz
* Ethernet controller: Intel Corporation 82576 Gigabit Network Connection (rev 01)

### Controller (1)

(eat1-app129.stg.linkedin.com)

* 24-core Intel(R) Xeon(R) CPU X5650  @ 2.67GHz
* Ethernet controller: Intel Corporation 82576 Gigabit Network Connection (rev 01)

### ZooKeeper (1)

(eat1-app87.corp.linkedin.com)

* 24-core Intel(R) Xeon(R) CPU E5645  @ 2.40GHz

Process
-------

A resource with 16 partitions will be created on a cluster with two participants.

First, we generate a set of fixed-sized strings. These remain fixed over the
course of the experiment in order to avoid overheads caused by garbage
collecting the messages. We just wrap them in ByteBuf objects in our codec. We
can vary the size of these strings to test how different message sizes perform.

Next, we write a driver that lives in each participant, which continually
selects one of these strings and sends it to a randomly selected partition.
Note that this randomly selected partition may live on either participant.

We run this configuration for a fixed period of time, while collecting logs and
metrics.
