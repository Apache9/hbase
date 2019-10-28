package com.xiaomi.infra.hbase.falcon;

public class FalconLatestResponse {
	private String counter;
	private String endpoint;
	private FalconValue value;

	public String getCounter() {
		return counter;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public FalconValue getValue() {
		return value;
	}
}
