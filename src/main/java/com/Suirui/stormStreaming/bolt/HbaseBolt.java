package com.Suirui.stormStreaming.bolt;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
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
import com.Suirui.stormStreaming.util.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Result;

@SuppressWarnings("deprecation")
public class HbaseBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3742575339677330780L;

	private static final Logger LOG = Logger.getLogger(HbaseBolt.class);

	// 瞩目上报信息表
	private String ZOOM_TABLE_NAME = "zoom-event";
	// 基础信息族
	private static final String ZOOM_BASE_INFO_FAMILY_NAME = "BaseInfo";
	// source信息族，把source拆分后插入
	private static final String ZOOM_SOURCE_FAMILY_NAME = "Source";
	// content信息
	private static final String ZOOM_CONTENT_FAMILY_NAME = "Content";
	// 错误信息表
	private static final String EXCEPTION_TABLE_NAME = "all-exception";
	// 错误包
	private static final String ERR_PACKAGE_FAMILY_NAME = "ErrPackage";

	private OutputCollector collector;
	private HConnection connection;
	private HTableInterface zoomMsgTable;
	private HTableInterface exceptionTable;
	private boolean persistAllEvents;

	private static final List<String> BASE_ARRY = new ArrayList<String>() {
		{
			add("app");
			add("project");
			add("module");
			add("level");
		}
	};
	private static final List<String> SOURCE_ARRY = new ArrayList<String>() {
		{
			add("source");
		}
	};
	private static final List<String> CONTENT_ARRY = new ArrayList<String>() {
		{
			add("content");
		}
	};

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
	public void execute(Tuple tuple) {

		LOG.info("About to insert tuple[" + tuple + "] into HBase...");

		String msgfiled = tuple.getStringByField("msg");
		try {
			msgfiled = URLDecoder.decode(msgfiled, "utf-8");
		} catch (UnsupportedEncodingException e1) {
			LOG.error(e1.getMessage());
		}
		LOG.info("msg = " + msgfiled);
		String id = RandomUtil.generateNum(10);

		while (true) {
			LOG.info("Test id" + id);
			try {
				Get get = new Get(Bytes.toBytes(id));
				Result ret = this.zoomMsgTable.get(get);

				if (ret.isEmpty()) {
					break;
				} else {
					id = RandomUtil.generateNum(10);
				}
			} catch (IOException e1) {
				LOG.error(e1.getMessage());
			}
		}

		Put put = new Put(Bytes.toBytes(id));
		String baseFamily = ZOOM_BASE_INFO_FAMILY_NAME;

		Map<String, String> data = convertPacket(msgfiled);
		boolean hasColumn = false;
		for (Map.Entry<String, String> entry : data.entrySet()) {
			LOG.info("key : " + entry.getKey());
			LOG.info("entry : " + entry.getValue());
			if (BASE_ARRY.contains(entry.getKey())) {
				put.addColumn(Bytes.toBytes(baseFamily), Bytes.toBytes(entry.getKey()),
						Bytes.toBytes(entry.getValue()));
				hasColumn = true;
			} else if (SOURCE_ARRY.contains(entry.getKey())) {
				Map<String, String> source = splitSource(entry.getValue());
				for (Map.Entry<String, String> sourceItem : source.entrySet()) {
					put.addColumn(Bytes.toBytes(ZOOM_SOURCE_FAMILY_NAME), Bytes.toBytes(sourceItem.getKey()),
							Bytes.toBytes(sourceItem.getValue()));
				}
				hasColumn = true;
			} else if (CONTENT_ARRY.contains(entry.getKey())) {

				String content = entry.getValue();
				try {
					content = URLDecoder.decode(content, "utf-8");
					content = StringUtils.replace(content, "\\", "");
					JSONObject contentObject = JSON.parseObject(content);
					for (Map.Entry<String, Object> contentIetm : contentObject.entrySet()) {
						put.addColumn(Bytes.toBytes(ZOOM_CONTENT_FAMILY_NAME), Bytes.toBytes(contentIetm.getKey()),
								Bytes.toBytes(URLEncoder.encode(contentIetm.getValue().toString(), "utf-8")));
					}
					hasColumn = true;
				} catch (Exception e) {
					hasColumn = false;
					LOG.error("handle content error!");
					LOG.error(e.getMessage());
					insertException(msgfiled);					
				}
			}

		}

		LOG.info("insert to hbase ..");
		try {
			if (hasColumn) {
				this.zoomMsgTable.put(put);
				LOG.info("Success inserting event into HBase table[" + ZOOM_TABLE_NAME + "]");
			} else {
				LOG.info("data Format error");
			}

		} catch (IOException e) {
			LOG.error("Error inserting event into HBase table[" + ZOOM_TABLE_NAME + "]");
		}
		collector.ack(tuple);
	}

	private void insertException(String errPackage) {
		String id = RandomUtil.generateNum(10);

		while (true) {
			LOG.info("Test id" + id);
			try {
				Get get = new Get(Bytes.toBytes(id));
				Result ret = this.exceptionTable.get(get);

				if (ret.isEmpty()) {
					break;
				} else {
					id = RandomUtil.generateNum(10);
				}
			} catch (IOException e1) {
				LOG.error(e1.getMessage());
			}
		}

		Put put = new Put(Bytes.toBytes(id));
		try {
			put.addColumn(Bytes.toBytes(ERR_PACKAGE_FAMILY_NAME), Bytes.toBytes("package"), Bytes.toBytes(errPackage));
			this.exceptionTable.put(put);
			LOG.info("Success inserting event into HBase table[" + EXCEPTION_TABLE_NAME + "]");
		} catch (IOException e) {
			LOG.info("fail to  inserting event into HBase table[" + EXCEPTION_TABLE_NAME + "]");
			LOG.error(e.getMessage());
		}
	}

	private Map<String, String> convertPacket(String content) {
		Map<String, String> ret = new HashMap<String, String>();
		if (StringUtils.isNotBlank(content)) {
			String[] args = StringUtils.split(content, "&");
			for (int i = 0; i < args.length; i++) {
				String pair[] = StringUtils.split(args[i], "=");
				if (pair.length == 2) {
					ret.put(pair[0], pair[1]);
				}
			}
		}
		return ret;
	}

	private Map<String, String> splitSource(String source) {
		Map<String, String> ret = new HashMap<String, String>();
		if (StringUtils.isNotBlank(source)) {
			String[] args = StringUtils.split(source, ";");
			for (int i = 0; i < args.length; i++) {
				String pair[] = StringUtils.split(args[i], ":");
				if (pair.length == 2) {
					ret.put(pair[0], pair[1]);
				}
			}
		}
		return ret;
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		try {
			// this.connection =
			/*
			 * HConnectionManager.createConnection(constructConfiguration());
			 * this.zoomMsgTable = connection.getTable(ZOOM_TABLE_NAME);
			 */
			this.connection = HConnectionManager.createConnection(constructConfiguration());

			String[] nameList = connection.getTableNames();
			if (nameList.length > 0) {
				boolean tempTableNameFlag = false;
				for (String tempTableName : nameList) {
					if (tempTableName.equals(ZOOM_TABLE_NAME)) {
						this.zoomMsgTable = connection.getTable(ZOOM_TABLE_NAME);
						tempTableNameFlag = true;
					}
					if (tempTableName.equals(EXCEPTION_TABLE_NAME)) {
						this.exceptionTable = connection.getTable(EXCEPTION_TABLE_NAME);
						tempTableNameFlag = true;
					}
				}
				if (tempTableNameFlag == false) {
					this.zoomMsgTable = connection.getTable(nameList[0]);
					ZOOM_TABLE_NAME = nameList[0];
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
