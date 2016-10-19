package com.Suirui.stormStreaming.bolt;

import java.io.IOException;
import java.util.ArrayList;
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

	private  String ZOOM_TABLE_NAME = "zoom";
	private static final String ZOOM_TABLE_COLUMN_FAMILY_NAME = "msg";

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

		LOG.info("About to insert tuple[" + arg0 + "] into HBase...");
		
		String msgfiled=arg0.getStringByField("msg");
		
		double randemD = Math.random() * 1000;
		Put put = new Put(Bytes.toBytes("zoom-suirui-19900326" + randemD));
		String columnFamily = ZOOM_TABLE_COLUMN_FAMILY_NAME;
	
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("firstAttri"),
					Bytes.toBytes("msgfiled"));
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
			// this.connection =
			/*HConnectionManager.createConnection(constructConfiguration());
			 this.zoomMsgTable = connection.getTable(ZOOM_TABLE_NAME);*/
			this.connection = HConnectionManager.createConnection(constructConfiguration());

			String[] nameList = connection.getTableNames();
			if (nameList.length > 0) {
				boolean tempTableNameFlag = false;
				for (String tempTableName : nameList) {
					if (tempTableName.equals(ZOOM_TABLE_NAME)) {
						this.zoomMsgTable = connection.getTable(ZOOM_TABLE_NAME);
						tempTableNameFlag = true;
					}
				}
				if(tempTableNameFlag==false){
					this.zoomMsgTable = connection.getTable(nameList[0]);
					ZOOM_TABLE_NAME=nameList[0];
					tempTableNameFlag = true;
				}
			} else {

			}

		} catch (Exception e) {
			String errMsg = "Error retrievinging connection and access to Table";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields arg0;

		ArrayList<String> tempArray = new ArrayList<String>();
		tempArray.add("firstAttri");
		arg0 = new Fields(tempArray);

		declarer.declare(arg0);

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {

		return null;
	}

	public static Configuration constructConfiguration() {
		Configuration config = HBaseConfiguration.create();
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		return config;
	}

}
