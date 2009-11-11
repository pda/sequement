#!/usr/bin/ruby -w

HOST = 'localhost'
PORT = 2345
DIR = 'tmp'
CONCURRENCY = 2

require 'sequement/init'
Sequement::Master.new(HOST, PORT, DIR, CONCURRENCY).start
