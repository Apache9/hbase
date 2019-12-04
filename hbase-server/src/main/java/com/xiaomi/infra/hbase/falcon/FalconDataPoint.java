/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xiaomi.infra.hbase.falcon;

public class FalconDataPoint {
	private static final String SEPARATOR = ",";
	private final String endpoint;
	private final String metric;
	private final long timestamp;
	private final int value;
	private final int step;
	private final String tags;
	private final String counterType = "GAUGE";

	private FalconDataPoint(Builder builder) {
		endpoint = builder.endpoint;
		metric = builder.metric;
		timestamp = builder.timestamp;
		value = builder.value;
		step = builder.step;
		tags = builder.tags;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return sb.append("endpoint=" + endpoint)
				.append("metric=" + metric).append(SEPARATOR)
				.append("timestamp=" + timestamp).append(SEPARATOR)
				.append("value=" + value).append(SEPARATOR)
				.append("step=" + step).append(SEPARATOR)
				.append("tags=" + tags).append(SEPARATOR)
				.append("counterType=" + counterType).append(SEPARATOR)
				.toString();
	}

	public static class Builder {
		private String endpoint;
		private String metric;
		private long timestamp;
		private int value;
		private int step;
		private String tags;

		public Builder setEndpoint(String endpoint) {
			this.endpoint = endpoint;
			return this;
		}

		public Builder setMetric(String metric) {
			this.metric = metric;
			return this;
		}

		public Builder setTimestamp(long timestamp) {
			this.timestamp = timestamp;
			return this;
		}

		public Builder setValue(int value) {
			this.value = value;
			return this;
		}

		public Builder setStep(int step) {
			this.step = step;
			return this;
		}

		public Builder setTags(String tags) {
			this.tags = tags;
			return this;
		}

		public FalconDataPoint build() {
			return new FalconDataPoint(this);
		}

	}
}
