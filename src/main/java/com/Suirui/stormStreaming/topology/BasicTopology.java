package com.Suirui.stormStreaming.topology;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class BasicTopology {

	  private static final Logger LOG = Logger.getLogger(BasicTopology.class);
	  protected Properties topologyConfig;

	  public BasicTopology(String configFileLocation) throws Exception {
	    topologyConfig = new Properties();
	    try {
	      topologyConfig.load(new FileInputStream(configFileLocation));
	    } catch (FileNotFoundException e) {
	      LOG.error("Encountered error while reading configuration properties: "
	          + e.getMessage());
	      throw e;
	    } catch (IOException e) {
	      LOG.error("Encountered error while reading configuration properties: "
	          + e.getMessage());
	      throw e;
	    }
	  }


}
