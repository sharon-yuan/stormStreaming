package com.Suirui.stormStreaming.util;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomUtil {
    public static String generateNum(int length){
    	return RandomStringUtils.randomNumeric(length);
    }
}
