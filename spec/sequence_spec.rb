#!/usr/bin/ruby -w

require 'sequement'

describe Sequement::Sequence do

  before :each do
    path = 'tmp/name'
    File.delete path if File.exists? path
  end

	it "should instantiate" do
		seq = Sequement::Sequence.new('name', 'tmp')
	end

	it "should increment" do
		seq = Sequement::Sequence.new('name', 'tmp')
		seq.next.should == 1
		seq.next.should == 2
	end

	it "should persist" do
		seq = Sequement::Sequence.new('name', 'tmp')
		seq.next.should == 1
		seq.next.should == 2
		seq.save_sequence

		seq = Sequement::Sequence.new('name', 'tmp')
		seq.next.should == 3
  end

  it "should not overlap after crash" do
		seq_precrash = Sequement::Sequence.new('name', 'tmp')
		seq_precrash.next.should == 1
		seq_precrash.next.should == 2

		seq_postcrash = Sequement::Sequence.new('name', 'tmp')
		seq_postcrash.next.should >= 3
  end

end