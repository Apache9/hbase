package com.xiaomi.infra.hbase.falcon;

public class FalconConstant {
	private static final String COMMON_URL_PREFIX = "http://api.falcon.srv/v1.0";
	public static final String QUERY_LATEST_URL = COMMON_URL_PREFIX + "/pub/graph/last";
	public static final String METRIC_LOAD_1MIN = "load.1min";

	public static final String JSON_CONTENT_TYPE = "application/json;charset=UTF-8";
}
