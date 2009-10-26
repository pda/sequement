#!/usr/bin/ruby -w

HOST = 'localhost'
PORT = 2345
CONCURRENCY = 2

require 'sequement'
Sequement::Master.new(HOST, PORT, CONCURRENCY).start
