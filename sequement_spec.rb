#!/usr/bin/ruby -w

require 'sequement/server'

describe Sequement do

	it "should instantiate" do
		sequement = Sequement::Server.new('localhost', 2345)
	end

end