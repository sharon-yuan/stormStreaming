package com.Suirui.stormStreaming.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class EventScheme implements Scheme{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4818308754234155130L;

	public List<Object> deserialize(byte[] arg0) {
		//TODO find the useful info from whole dataflow
		 try {
	            String msg = new String(arg0, "UTF-8"); 
	            return new Values(msg);
	        } catch (UnsupportedEncodingException e) {  
	         
	        }
	        return null;
	}

	public Fields getOutputFields() {
		  return new Fields("msg");  
	}

}
