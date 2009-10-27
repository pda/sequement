#!/usr/bin/ruby -w

require 'sequement'

describe Sequement::Sequence do

  def delete_sequence(dir, name)
    path = '%s/%s' % [dir, name]
    File.delete path if File.exists? path
  end

	it "should instantiate" do
		delete_sequence 'tmp', 'name'
		seq = Sequement::Sequence.new('name', 'tmp')
	end

	it "should increment" do
		delete_sequence 'tmp', 'name'
		seq = Sequement::Sequence.new('name', 'tmp')
		seq.next.should == 1
		seq.next.should == 2
	end

	it "should persist" do
		delete_sequence 'tmp', 'name'
		seq = Sequement::Sequence.new('name', 'tmp')
		seq.next.should == 1
		seq.next.should == 2
		seq.save_sequence

		seq = Sequement::Sequence.new('name', 'tmp')
		seq.next.should == 3
  end

  it "should not overlap after crash" do
		delete_sequence 'tmp', 'name'
		seq_precrash = Sequement::Sequence.new('name', 'tmp')
		seq_precrash.next.should == 1
		seq_precrash.next.should == 2

		seq_postcrash = Sequement::Sequence.new('name', 'tmp')
		seq_postcrash.next.should >= 3
  end

end