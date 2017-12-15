package com.split.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class WordCountBolt implements IRichBolt {

	private HashMap<String, Long> counts = null;
	private OutputCollector collector;

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		String str = tuple.getStringByField("word");
		if (!counts.containsKey(str)) {
			counts.put(str, 1L);
		} else {
			Long c = counts.get(str) + 1;
			counts.put(str, c);
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("word", "count"));
	}

	public void cleanup() {

		System.out.println("*****************FINAL OUTPUT*****************");
		for (Map.Entry<String, Long> entry : counts.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		System.out.println("**************THE END**************");
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}