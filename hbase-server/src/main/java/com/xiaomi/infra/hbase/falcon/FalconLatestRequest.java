package com.xiaomi.infra.hbase.falcon;

public class FalconLatestRequest {
	private String endpoint;
	private String counter;

	private FalconLatestRequest(String endpoint, String counter) {
		this.endpoint = endpoint;
		this.counter = counter;
	}

	public static class Builder {
		private String endpoint;
		private String counter;

		public Builder(String endpoint, String counter) {
			this.endpoint = endpoint;
			this.counter = counter;
		}

		public FalconLatestRequest build() {
			return new FalconLatestRequest(endpoint, counter);
		}
	}

	@Override
	public String toString() {
		return "FalconLatestRequest(endpoint=" + endpoint + ",counter=" + counter + ")";
	}
}
