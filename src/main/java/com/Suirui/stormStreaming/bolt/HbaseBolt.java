package com.Suirui.stormStreaming.bolt;

import java.util.Properties;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;

public class HbaseBolt {
	private static final Logger LOG = Logger.getLogger(HbaseBolt.class);
	
	private static final String RP_TABLE_NAME="rp_msg";
	private static final String RP_TABLE_COLUMN_FAMILY_NAME="msg";
	
	private static final String HUIJIAN_TABLE_NAME="huijian_msg";
	private static final String HUIJIAN_TABLE_COLUMN_FAMILY_NAME="msg";
	
	private static final String ZOOM_TABLE_NAME="zoom_msg";
	private static final String ZOOM_TABLE_COLUMN_FAMILY_NAME="msg";
	
	private OutputCollector collector;
	@SuppressWarnings("deprecation")
	private HConnection connection;
	@SuppressWarnings("deprecation")
	private HTableInterface rpMsgTable;
	@SuppressWarnings("deprecation")
	private HTableInterface huijianMsgTable;
	@SuppressWarnings("deprecation")
	private HTableInterface zoomMsgTable;
	
	private boolean persistAllEvents;
	
	
	public HbaseBolt(Properties topologyConfig) {
		this.persistAllEvents = Boolean.valueOf(topologyConfig.getProperty("hbase.persist.all.events")).booleanValue();
		LOG.info("The PersistAllEvents Flag is set to: " + persistAllEvents);
	}


}
