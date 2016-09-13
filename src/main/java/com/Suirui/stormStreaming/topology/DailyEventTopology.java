package com.Suirui.stormStreaming.topology;

import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.format.RecordFormat;

import com.Suirui.stormStreaming.bolt.FileTimeRotationPolicy;
import com.Suirui.stormStreaming.bolt.HbaseBolt;
import com.Suirui.stormStreaming.bolt.HiveTablePartitionAction;
import com.Suirui.stormStreaming.spout.EventScheme;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class DailyEventTopology extends BasicTopology {
	private static final Logger LOG = Logger.getLogger(DailyEventTopology.class);

	public DailyEventTopology(String configFileLocation) throws Exception {
		super(configFileLocation);
	}

	public static void main(String[] args) throws Exception {
		String configFileLocation;
		if (args!=null&&args.length>0){
			System.out.println(args.length);
			for(String astring:args)System.out.println(astring);
		configFileLocation = args[0];}
		else configFileLocation=null;
		DailyEventTopology truckTopology = new DailyEventTopology(configFileLocation);
		truckTopology.WSYBuildAndSubmit();
	}

	@SuppressWarnings("deprecation")
	private void WSYBuildAndSubmit() {
		TopologyBuilder builder = new TopologyBuilder();
		LOG.info("start config");
		configureKafkaSpout(builder);
		if (topologyConfig.getProperty("kafka.topic").contains("msg"))
			{
			LOG.info("start HBaseconfig");
			configureHBaseBolt(builder);
			}
		else
			;// TODO 
		configureHDFSBolt(builder);
		Config conf = new Config();
		conf.setDebug(true);
		/*
		 * Set the number of workers that will be spun up for this topology.
		 * Each worker represents a JVM where executor thread will be spawned
		 * from
		 */
		LOG.info("get storm.daily.topology.workers");
		Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("storm.daily.topology.workers"));
		conf.put(Config.TOPOLOGY_WORKERS, topologyWorkers);

		// Read the nimbus host in from the config file as well
		LOG.info("get nimbus.host");
		String nimbusHost = topologyConfig.getProperty("nimbus.host");
		conf.put(Config.NIMBUS_HOST, nimbusHost);

		try {
			LOG.info("starting submit topology");
			StormSubmitter.submitTopology("zoom-msg-processor", conf, builder.createTopology());
			LOG.info("ending submit topology");
		} catch (Exception e) {
			LOG.error("Error submiting Topology", e);
		}

	}

	
	
	public int configureKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());

		int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
		int boltCount = Integer.valueOf(topologyConfig.getProperty("bolt.thread.count"));

		builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
		return boltCount;
	}

	

	private SpoutConfig constructKafkaSpoutConf() {
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
		String topic = topologyConfig.getProperty("kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = topologyConfig.getProperty("kafka.consumer.group.id");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

		/*
		 * Custom TruckScheme that will take Kafka message of single truckEvent
		 * and emit a 2-tuple consisting of truckId and truckEvent. This
		 * driverId is required to do a fieldsSorting so that all driver events
		 * are sent to the set of bolts
		 */
		spoutConfig.scheme = new SchemeAsMultiScheme(new EventScheme());

		return spoutConfig;
	}

	public void configureHBaseBolt(TopologyBuilder builder) {
		HbaseBolt hbaseBolt = new HbaseBolt(topologyConfig);
		builder.setBolt("hbase_bolt", hbaseBolt, 2).shuffleGrouping("kafkaSpout");
	}

	
	  public void configureHDFSBolt(TopologyBuilder builder) {
			// Use pipe as record boundary

			String rootPath = topologyConfig.getProperty("hdfs.path");
			String prefix = topologyConfig.getProperty("hdfs.file.prefix");
			String fsUrl = topologyConfig.getProperty("hdfs.url");
			String sourceMetastoreUrl = topologyConfig.getProperty("hive.metastore.url");
			String hiveStagingTableName = topologyConfig.getProperty("hive.staging.table.name");
			String databaseName = topologyConfig.getProperty("hive.database.name");
			Float rotationTimeInMinutes = Float.valueOf(topologyConfig.getProperty("hdfs.file.rotation.time.minutes"));

			RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");

			// Synchronize data buffer with the filesystem every 1000 tuples
			SyncPolicy syncPolicy = new CountSyncPolicy(1000);

			// Rotate data files when they reach five MB
			// FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f,
			// Units.MB);

			// Rotate every X minutes
			FileTimeRotationPolicy rotationPolicy = new FileTimeRotationPolicy(rotationTimeInMinutes,
					FileTimeRotationPolicy.Units.MINUTES);

			// Hive Partition Action
			HiveTablePartitionAction hivePartitionAction = new HiveTablePartitionAction(sourceMetastoreUrl,
					hiveStagingTableName, databaseName, fsUrl);

			// MoveFileAction moveFileAction = new
			// MoveFileAction().toDestination(rootPath + "/working");

			FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(rootPath + "/staging").withPrefix(prefix);

			// Instantiate the HdfsBolt
			HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(fsUrl).withFileNameFormat(fileNameFormat).withRecordFormat(format)
					.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy).addRotationAction(hivePartitionAction);

			int hdfsBoltCount = Integer.valueOf(topologyConfig.getProperty("hdfsbolt.thread.count"));
			builder.setBolt("hdfs_bolt", hdfsBolt, hdfsBoltCount).shuffleGrouping("kafkaSpout");
		}
	 

}
