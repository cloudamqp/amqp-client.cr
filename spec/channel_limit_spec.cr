require "./spec_helper"

describe "channel_max" do
  it "should not open more channels than channel_max" do
    with_connection(channel_max: 10_u16) do |c|
      c.channel_max.should eq 10_u16
      channels = [] of AMQP::Client::Channel
      10.times do
        channels << c.channel
      end
      expect_raises(Exception, "channel_max reached") do
        c.channel
      end
      channels.each &.close
    end
  end

  it "should use client's channel_max when it's lower than server's" do
    # LavinMQ defaults to channel_max=2048
    with_connection(channel_max: 10_u16) do |c|
      c.channel_max.should eq 10_u16
    end
  end

  it "should use server's channel_max when it's lower than client's" do
    # LavinMQ defaults to channel_max=2048
    with_connection(channel_max: 4096_u16) do |c|
      c.channel_max.should eq 2048_u16
    end
  end

  it "should use server's channel_max when client's is 0 (unlimited)" do
    # LavinMQ defaults to channel_max=2048
    with_connection(channel_max: 0_u16) do |c|
      c.channel_max.should eq 2048_u16
    end
  end
end
