package com.sentence.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector outputCollector;
	private String sentences[] = { "Each day provides its own gifts", "I like cold/hot beverages", "This is Storm program",
			"don't have a cow man", "Be firm on ground" };

	private int index = 0;

	@Override
	public void nextTuple() {
		//Utils.sleep(1000);
		outputCollector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(1000);
	}

	@Override
	// Used For Initialization
	public void open(Map map, TopologyContext context, SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));

	}

}
