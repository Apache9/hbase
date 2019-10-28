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

public class HttpConnectionParams {
  private int maxTotal;
  private int defaultMaxPerRoute;

	private HttpConnectionParams(Builder builder) {
		maxTotal = builder.maxTotal;
		defaultMaxPerRoute = builder.defaultMaxPerRoute;
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public int getDefaultMaxPerRoute() {
		return defaultMaxPerRoute;
	}

	public static class Builder {
		private int maxTotal = 100;
		private int defaultMaxPerRoute = 5;

		public Builder() {}

		public Builder setMaxTotal(int maxTotal) {
			this.maxTotal = maxTotal;
			return this;
		}

		public Builder setDefaultMaxPerRoute(int defaultMaxPerRoute) {
			this.defaultMaxPerRoute = defaultMaxPerRoute;
			return this;
		}

		public HttpConnectionParams build() {
			return new HttpConnectionParams(this);
		}
	}
}
