package com.xiaomi.infra.hbase.falcon;

import java.util.List;

public class FalconLatestResponse {
	private String msg;
	private List<List<Double>> data;

	public List<List<Double>> getLoad() {
		return data;
	}
}
