package com.word.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.sentence.spout.SentenceSpout;
import com.split.bolt.ReportBolt;
import com.split.bolt.SplitBolt;
import com.split.bolt.WordCountBolt;

public class WordCountTopology {

	public static void main(String[] args) throws Exception {
		// Used For Construction of Topologies
		TopologyBuilder builder = new TopologyBuilder();

		// Defines New Spout in Topology
		builder.setSpout("sentence-spout", new SentenceSpout());

		// Defines New Bolt in Topology
		builder.setBolt("split-bolt", new SplitBolt()).shuffleGrouping("sentence-spout");

		builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));
		builder.setBolt("report-bolt", new ReportBolt()).globalGrouping("count-bolt");

		// Represents set of configuration parameters global to all Components
		// in Topology.

		Config config = new Config();

		// Used for Running Topologies in Local Environment
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("word-count-topology", config, builder.createTopology());
		Thread.sleep(18000);
		localCluster.shutdown();

	}

}
