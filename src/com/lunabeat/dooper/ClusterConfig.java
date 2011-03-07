/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lunabeat.dooper;

import com.amazonaws.auth.AWSCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author cory
 */
public class ClusterConfig implements AWSCredentials {

	private final Properties _properties = new Properties();
	private final Properties _requiredFields = new Properties();
	private final static String REQUIRED_FIELDS = "/com/lunabeat/dooper/RequiredConfigFields.properties";
	public static final String SECRET_KEY_KEY = "AWS.SecretKey";
	public static final String ACCESS_KEY_KEY = "AWS.AccessKey";
	public static final String ACCOUNT_ID_KEY = "AWS.AccountId";
	public static final String DEFAULT_AMI_KEY = "AMI.DefaultImage";
	public static final String USER_DATA_PATH_KEY = "EC2.UserDataFile";
	public static final String KEYPAIR_NAME_KEY = "EC2.KeypairName";
	public static final String KEYPAIR_FILE_KEY = "EC2.KeypairFile";
	public static final String MASTER_HOST_KEY = "Master.Host";
	public static final String WEB_PORTS_KEY = "EC2.WebPorts";
	public static final String USERNAME_KEY = "EC2.Username";
	public static final List<String> INSTANCE_TYPES = initInstanceTypes();
	private static List<String> initInstanceTypes(){
		ArrayList<String> tmpList = new ArrayList<String>();
		tmpList.add("t1.micro");
		tmpList.add("m1.small");
		tmpList.add("m1.large");
		tmpList.add("m1.xlarge");
		tmpList.add("m2.xlarge");
		tmpList.add("m2.2xlarge");
		tmpList.add("m2.4xlarge");
		tmpList.add("c1.medium");
		tmpList.add("c1.xlarge");
		tmpList.add("cc1.4xlarge");
		tmpList.add("cg1.4xlarge");
		return Collections.unmodifiableList(tmpList);
	}
	public static final List<String> INSTANCE_GROUP_TYPES = initInstaceGroupTypes();
	static private List<String> initInstaceGroupTypes(){
		ArrayList<String> tmpList = new ArrayList<String>();
		tmpList.add("cluster");
		tmpList.add("master");
		tmpList.add("slaves");
		return Collections.unmodifiableList(tmpList);
	}
	 
	public static final String SCP_FILE_MODE = "0644";

	
	/**
	 * 
	 * @param src
	 * @throws IOException
	 * @throws com.lunabeat.dooper.ClusterConfig.ConfigException
	 */
	public ClusterConfig(InputStream src) throws IOException, ConfigException {
		_properties.load(src);
		init();
	}

	/**
	 *
	 * @param resourcePath
	 * @throws IOException
	 * @throws com.lunabeat.dooper.ClusterConfig.ConfigException
	 */
	public ClusterConfig(String path) throws IOException, ConfigException {
		FileInputStream fis = new FileInputStream(path);
		_properties.load(fis);
		init();
	}

	private void testConfig() throws ConfigException {
		ArrayList<String> errs = new ArrayList<String>();
		for (String key : _requiredFields.stringPropertyNames()) {
			String type = _requiredFields.getProperty(key);
			String confValue = _properties.getProperty(key);
			if (confValue == null) {
				errs.add(key + " missing (" + type + ")");
			}
		}
		if (errs.size() > 0) {
			throw new ConfigException(errs.toString());
		}

	}

	public String get(String name) {
		return _properties.getProperty(name);
	}

	public String get(String name, String defaultVal) {
		return _properties.getProperty(name, defaultVal);
	}

	/**
	 * 
	 * @param name
	 * @return integer value of property or null
	 */
	public Integer getInt(String name) {
		Integer result = null;
		try {
			Integer.parseInt(_properties.getProperty(name));
		} catch (NumberFormatException e) {
			//log it, I guess
		}
		return result;
	}

	private void init() throws IOException, ConfigException {
		_requiredFields.load(ClusterConfig.class.getResourceAsStream(REQUIRED_FIELDS));
		testConfig();

	}

	public String getAWSAccessKeyId() {
		return get(ACCESS_KEY_KEY);
	}

	public String getAWSSecretKey() {
		return get(SECRET_KEY_KEY);
	}

	public static class ConfigException extends Exception {

		ConfigException(String message) {
			super(message);
		}
	}

	public Map<String, String> getRequiredFields() {
		HashMap<String, String> fields = new HashMap<String, String>();
		for (String key : _requiredFields.stringPropertyNames()) {
			fields.put(key, _requiredFields.getProperty(key));
		}
		return fields;
	}
}
