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

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.xiaomi.infra.hbase.falcon.FalconConstant;

/**
 * This class is about Http client utils.
 * You can customize parameters in HttpConnectionParams and HttpRequestParams, such as pool size,
 * timeout etc.
 * You can keep the long-live HttpClient and close it yourself or use short-live in not so high
 * performance cases.
 */
public class HttpClientUtils {
	private static final Log LOG = LogFactory.getLog(HttpClientUtils.class);
	private static final Gson GSON = new Gson();

	public static CloseableHttpClient newHttpClient(HttpConnectionParams connectionParams) {
		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setMaxTotal(connectionParams.getMaxTotal());
		connectionManager.setDefaultMaxPerRoute(connectionParams.getDefaultMaxPerRoute());
		return HttpClients.custom().setConnectionManager(connectionManager).build();
	}

	public static CloseableHttpClient newHttpClient() {
		HttpConnectionParams httpConnectionParams = new HttpConnectionParams.Builder().build();
		return newHttpClient(httpConnectionParams);
	}

	public static <T> String post(CloseableHttpClient client, String uri,
			HttpRequestParams httpRequestParams, T postObject) throws IOException {
		HttpPost httpPost = new HttpPost(uri);
		httpPost.setHeader("Content-Type", FalconConstant.JSON_CONTENT_TYPE);
		RequestConfig requestConfig =
				RequestConfig.custom().setConnectTimeout(httpRequestParams.getConnectionTimeout())
						.setSocketTimeout(httpRequestParams.getSocketTimeout()).build();
		httpPost.setConfig(requestConfig);
		String postBody = GSON.toJson(Arrays.asList(postObject));
		StringEntity se = new StringEntity(postBody);
		se.setContentType("text/json");
		httpPost.setEntity(se);
		CloseableHttpResponse response = client.execute(httpPost);
		HttpEntity httpEntity = response.getEntity();
		return EntityUtils.toString(httpEntity, "UTF-8");
	}

	public static <T> String post(CloseableHttpClient client, String uri, T postBody)
			throws IOException {
		HttpRequestParams httpRequestParams = new HttpRequestParams.Builder().build();
		return post(client, uri, httpRequestParams, postBody);
	}

	public static <T> String post(String uri, T postBody) {
		CloseableHttpClient httpClient = newHttpClient();
		HttpRequestParams httpRequestParams = new HttpRequestParams.Builder().build();
		String response = null;
		try {
			response = post(httpClient, uri, httpRequestParams, postBody);
		} catch (IOException e) {
			LOG.error("Failed to post body.URI " + uri + " HttpRequestParams "
					+ httpRequestParams + " PostBody " + postBody, e);
			return null;
		} finally {
			try {
				httpClient.close();
			} catch (IOException e) {
				LOG.error("Failed to close HttpClient.", e);
				return null;
			}
		}
		return response;
	}
}
