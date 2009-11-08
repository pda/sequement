Sequement
=========

An experimental forking sequence server with asynchronous persistence,
written in Ruby by [Paul Annesley][1].

Simply tracks and increments named sequences, for example as a more scalable
replacement for database [auto increment][2] or [sequence][3] functionality.

Extensive use of Unix system calls like fork(), pipe() and select() is made,
as inspired by the [Unicorn][4] HTTP server.

  [1]: http://paul.annesley.cc/
  [2]: http://dev.mysql.com/doc/refman/5.0/en/example-auto-increment.html
  [3]: http://www.postgresql.org/docs/current/interactive/sql-createsequence.html
  [4]: http://unicorn.bogomips.org/

Status
------

A functional work in progress.

  * Known to run on Ubuntu 9.10
  * Known to not yet run on Mac OS X

Eventually any platform on which Ruby's fork(), pipe(), select(), trap() etc
methods function correctly should be supported.

Internals
---------

The master process:

  * creates a listening TCP socket,
  * forks one 'writer' process for async disk persistence,
  * forks N 'workers' to handle inbound connections,
  * monitors pipes into each worker for IPC commands.
  * manages sequences, pre-reserving blocks of sequences on disk.

The worker processes:

  * accept an inbound connection,
  * receive a command e.g. 'next example' from the client,
  * send a light IPC command to the master requesting the next number in the sequence named 'example',
  * pass the response back to the client.

The writer process:

  * waits for light IPC commands instructing it to async write sequences to files.

Sequence Persistence
--------------------

Sequences are first pre-reserved in configurable sized blocks on disk, and
then tracked in memory.  For example on the first request for sequence named
'example', the number 100 (configurable) will be written to the 'example'
sequence file, but the number 1 will be served to the client.

Subsequent requests for the 'example' sequence will be incremented in memory
only, until a threshold is reached and another block of 100 is written to
disk.  The current actual value is only ever written to disk during a clean
shutdown.

By this mechanism, a crash would result in a sequence on disk *beyond* the
current actual value, causing usually-safe gaps, rather than often-catastrophic
overlapping.
