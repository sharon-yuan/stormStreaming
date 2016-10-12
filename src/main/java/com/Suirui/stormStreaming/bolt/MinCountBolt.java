package com.Suirui.stormStreaming.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MinCountBolt  implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Object emitFrequencyInSeconds = 60;
	private static int tupleCount=0;
	private static final Logger LOG = Logger.getLogger(MinCountBolt.class);
	private OutputCollector collector;
	@Override
	public void cleanup() {
		// TODO MinCountBolt cleanup
		
	}

	@Override
	public void execute(Tuple tuple) {
		
	    if (TupleHelpers.isTickTuple(tuple)) {  
	    	LOG.info("tick tuple: " + tuple);  
	        emitCountingData(collector,tuple);  
	    } else {  
	        countInLocal(tuple);  
	    }  
	    
	}  

	private void countInLocal(Tuple tuple) {
		tupleCount++;
		
	}

	private void emitCountingData(OutputCollector collector,Tuple tuple) {
		
		collector.emit(new Values(tupleCount));
		tupleCount=0;
		collector.ack(tuple);
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO MinCountBolt prepare
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg"));
		
		
	}

	@Override  
	public Map<String, Object> getComponentConfiguration() {  
	    Map<String, Object> conf = new HashMap<String, Object>();  
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);  
	    return conf;  
	}  


}
