/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest.filter;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class NamespaceAuthFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceAuthFilter.class);

  public static final String NAMESPACE_AUTH_KEY = "hbase.rest.authentication.namespace";

  public static final String NAMESPACE_PARAM_KEY = "namespace";

  private StringBuffer namespace;

  @Override
  public void init(FilterConfig filterConfig) {
    String namespaceParam = filterConfig.getInitParameter(NAMESPACE_PARAM_KEY);
    LOG.info("namespace auth value:{}", namespaceParam);
    namespace = new StringBuffer(namespaceParam);
  }

  /**
   * table uri example:
   * 1. "/namespace:table/rowKey"
   * 2. "/namespace:table/schema"
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
    FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    String uri = httpRequest.getRequestURI();
    if (!uri.contains(":")) {
      httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Only table api is allowed");
      LOG.warn("Access to non-table api is denied, url:{}", getRequestURL(httpRequest));
      return;
    } else if (!uri.split(":")[0].replace("/", "").equals(namespace.toString())) {
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
        "You can only access the tables under your own namespace");
      LOG.warn("Access to unauthorized namespace is denied, url:{}", getRequestURL(httpRequest));
      return;
    }
    filterChain.doFilter(httpRequest, httpResponse);
  }

  private String getRequestURL(HttpServletRequest request) {
    StringBuffer sb = request.getRequestURL();
    if (request.getQueryString() != null) {
      sb.append("?").append(request.getQueryString());
    }
    return sb.toString();
  }

  @Override
  public void destroy() {
  }

}
