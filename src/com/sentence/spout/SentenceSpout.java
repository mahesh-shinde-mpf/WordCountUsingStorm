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
	private String sentences[] = { "Each day provides its own gifts",
			"Storm is about real-time processing of data streams", "This is Storm program", "Don't have a cow man",
			"Be firm on ground", "colorful" };

	private int index = 0;

	//For sending out next tuple to output collector
	@Override
	public void nextTuple() {
		outputCollector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(1000);
	}

	
	/* Used For Initialization.
	 Context provides info about components in topology such as task ids,
	 inputs and outputs, etc.
	 Map is used for configuration of Storm.*/	
	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;

	}

	
	/* To tell Storm what streams a component will emit and the fields each
	 stream's tuples will contain.*/	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));

	}

}
