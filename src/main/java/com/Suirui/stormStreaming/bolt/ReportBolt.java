package com.Suirui.stormStreaming.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {
	/**
		 * 
		 */
	private static final Logger LOG = Logger.getLogger(ReportBolt.class);
	private static final long serialVersionUID = 1L;
	private HashMap<Integer, Integer> countMap = null;

	public void execute(Tuple arg0) {
		int timer = arg0.getIntegerByField("timer");
		int count = arg0.getIntegerByField("count");
		this.countMap.put(timer, count);

	}

	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		countMap = new HashMap<>();

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// this bolt has nothing to emit

	}

	public void cleanup() {
		System.out.println("--------report count result--------");
		List<Integer> timers = new ArrayList<Integer>();
		timers.addAll(this.countMap.keySet());
		Collections.sort(timers);
		for (int timer : timers) {
			LOG.info(timer + " : " + this.countMap.get(timer));
			System.out.println(timer + " : " + this.countMap.get(timer));
		}
		System.out.println("--------report count result ended--------");
	}

}
