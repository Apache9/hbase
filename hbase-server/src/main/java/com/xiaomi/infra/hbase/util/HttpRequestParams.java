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

package com.xiaomi.infra.hbase.util;

public class HttpRequestParams {
	private int connectionTimeout;
	private int socketTimeout;
	private int connectionRequestTimeout;

	private HttpRequestParams(Builder builder) {
		connectionTimeout = builder.connectionTimeout;
		socketTimeout = builder.socketTimeout;
		connectionRequestTimeout = builder.connectionRequestTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public int getConnectionRequestTimeout() {
		return connectionRequestTimeout;
	}

	public static class Builder {
		private int connectionTimeout = 5000;
		private int socketTimeout = 5000;
		private int connectionRequestTimeout = 5000;

		public Builder setConnectionTimeout(int connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
			return this;
		}

		public Builder setSocketTimeout(int socketTimeout) {
			this.socketTimeout = socketTimeout;
			return this;
		}

		public Builder setConnectionRequestTimeout(int connectionRequestTimeout) {
			this.connectionRequestTimeout = connectionRequestTimeout;
			return this;
		}

		public HttpRequestParams build() {
			return new HttpRequestParams(this);
		}
	}
}
