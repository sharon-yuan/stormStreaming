package com.Suirui.stormStreaming.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("deprecation")
public class HbaseBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3742575339677330780L;

	private static final Logger LOG = Logger.getLogger(HbaseBolt.class);

	private static final String ZOOM_TABLE_NAME = "zoom";
	private static final String ZOOM_TABLE_COLUMN_FAMILY_NAME = "msg";
private Tuple inputTuple=null;
	private OutputCollector collector;

	private HConnection connection;
	private HTableInterface zoomMsgTable;

	private boolean persistAllEvents;

	public HbaseBolt(Properties topologyConfig) {
		this.persistAllEvents = Boolean.valueOf(topologyConfig.getProperty("hbase.persist.all.events")).booleanValue();
		LOG.info("The PersistAllEvents Flag is set to: " + persistAllEvents);
	}

	@Override
	public void cleanup() {

		try {
			zoomMsgTable.close();
			connection.close();
		} catch (Exception e) {
			LOG.error("Error closing connections", e);
		}
	

	}

	@Override
	public void execute(Tuple arg0) {
		inputTuple=arg0;
		LOG.info("About to insert tuple[" + arg0 + "] into HBase...");
		List<String> listFileds = arg0.getFields().toList();
		Put put = new Put(Bytes.toBytes("zoom-suirui-19900326"));
		String columnFamily = ZOOM_TABLE_COLUMN_FAMILY_NAME ;
		for (String aString : listFileds)
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(aString),
					Bytes.toBytes(arg0.getStringByField(aString)));
		try {
			this.zoomMsgTable.put(put);
			LOG.info("Success inserting event into HBase table[" + ZOOM_TABLE_NAME + "]");

		} catch (IOException e) {
			LOG.error("Error inserting event into HBase table[" + ZOOM_TABLE_NAME + "]");

			e.printStackTrace();
		}
		collector.ack(arg0);
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {


		this.collector = collector;
		try {
			this.connection = HConnectionManager.createConnection(constructConfiguration());
			this.zoomMsgTable = connection.getTable(ZOOM_TABLE_NAME);
		
		} catch (Exception e) {
			String errMsg = "Error retrievinging connection and access to dangerousEventsTable";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields arg0;
		if(inputTuple!=null){
			
		 arg0=new Fields(inputTuple.getFields().toList());}
		else {
			LOG.error("inputTuple == null");
			ArrayList<String>tempArray=new ArrayList<>();
			tempArray.add("firstAttri");
			arg0=new Fields(tempArray);
			}
		declarer.declare(arg0);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		
		return null;
	}
	public static Configuration constructConfiguration() {
		Configuration config = HBaseConfiguration.create();
		return config;
	}

}
