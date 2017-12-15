package com.sentence.spout;

import java.util.Map;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout implements IRichSpout {

	private SpoutOutputCollector outputCollector;
	
	private String sentences[] = { "Each day provides its own gifts",
			"Storm is about real-time processing of data streams", "This is Storm program", "Don't have a cow man",
			"Be firm on ground", "colorful" };

	private int index = 0;

	
	 /*Used For Initialization. Context provides info about components in topology such as 
	  task id's, inputs and outputs, etc. Map is used for configuration of Storm.*/	 
	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;

	}

	// For sending out next tuple to output collector
	@Override
	public void nextTuple() {
		Object msgId = UUID.randomUUID().toString() + System.currentTimeMillis();
		outputCollector.emit(new Values(sentences[index]), msgId);
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(1000);
	}

	/*
	 * To tell Storm what streams a component will emit and the fields each
	 * stream's tuples will contain. */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));

	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void ack(Object msgId) {
		System.out.println("-----[WordCountUsingStorm]------ Processed Spouts:" + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("-----[WordCountUsingStorm]------ Failed Spouts:" + msgId);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
