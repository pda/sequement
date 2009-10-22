#!/usr/bin/ruby -w

require 'server'
Sequement::Server.new('localhost', 2345).run
