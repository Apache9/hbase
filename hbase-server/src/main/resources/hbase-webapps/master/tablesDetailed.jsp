<%--
/**
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
--%>
<%@ page contentType="text/html;charset=UTF-8"
         import="static org.apache.commons.lang.StringEscapeUtils.escapeXml"
         import="com.google.common.base.Strings"
         import="java.util.ArrayList"
         import="java.util.List"
         import="java.util.Map" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.hadoop.hbase.HBaseConfiguration" %>
<%@ page import="org.apache.hadoop.hbase.HRegionInfo" %>
<%@ page import="org.apache.hadoop.hbase.HTableDescriptor" %>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.master.RegionState" %>
<%@ page import="org.apache.hadoop.hbase.tmpl.master.MasterStatusTmplImpl" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>HBase Master: <%= master.getServerName() %>
  </title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="">
  <meta name="author" content="">


  <link href="/static/css/bootstrap.min.css" rel="stylesheet">
  <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
  <link href="/static/css/hbase.css" rel="stylesheet">
</head>

<body>

<div class="navbar  navbar-fixed-top navbar-default">
  <div class="container">
    <div class="navbar-header">
      <button type="button" class="navbar-toggle" data-toggle="collapse"
              data-target=".navbar-collapse">
        <span class="icon-bar"></span>
        <span class="icon-bar"></span>
        <span class="icon-bar"></span>
      </button>
      <a class="navbar-brand" href="/master-status"><img
          src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
    </div>
    <div class="collapse navbar-collapse">
      <ul class="nav navbar-nav">
        <li class="active"><a href="/master-status">Home</a></li>
        <li><a href="/tablesDetailed.jsp">Table Details</a></li>
        <li><a href="/logs/">Local Logs</a></li>
        <li><a href="/logLevel">Log Level</a></li>
        <li><a href="/dump">Debug Dump</a></li>
        <li><a href="/jmx">Metrics Dump</a></li>
        <% if (HBaseConfiguration.isShowConfInServlet()) { %>
        <li><a href="/conf">HBase Configuration</a></li>
        <% } %>
      </ul>
    </div><!--/.nav-collapse -->
  </div>
</div>

<div class="container">
  <div class="row inner_header">
    <div class="page-header">
      <h1>User Tables</h1>
    </div>
  </div>

  <% List<HTableDescriptor> tables;
    int tableLength = 0;
    String pageIndexStr = request.getParameter("pageIndex");
    String pageSizeStr = request.getParameter("pageSize");
    int pageIndex = Strings.isNullOrEmpty(pageIndexStr) ? 1 : Integer.parseInt(pageIndexStr);
    int pageSize = Strings.isNullOrEmpty(pageSizeStr) ? 100 : Integer.parseInt(pageSizeStr);
    tables = new ArrayList<HTableDescriptor>();
    String errorMessage = MasterStatusTmplImpl.getUserTables(master, tables);
    if (tables.size() == 0 && errorMessage != null) { %>
  <p><%= errorMessage %>
  </p>
  <%
    } else {
      tableLength = tables.size();
      int startIndex = (pageIndex - 1) * pageSize;
      int endIndex = Math.min(tables.size(), startIndex + pageSize);
      tables = tables.subList(startIndex, endIndex);
    }
    if (tables != null && tables.size() > 0) {
  %>
  <table class="table table-striped">
    <tr>
      <th>Namespace</th>
      <th>Table Name</th>
      <th>Online Regions</th>
      <th>Offline Regions</th>
      <th>Failed Regions</th>
      <th>Split Regions</th>
      <th>Other Regions</th>
      <th>Description</th>
    </tr>
    <%for (HTableDescriptor htDesc : tables) {%>
    <%
      Map<RegionState.State, List<HRegionInfo>> tableRegions =
          master.getAssignmentManager().getRegionStates()
              .getRegionByStateOfTable(htDesc.getTableName());
      int openRegionsCount = tableRegions.get(RegionState.State.OPEN).size();
      int offlineRegionsCount = tableRegions.get(RegionState.State.OFFLINE).size();
      int splitRegionsCount = tableRegions.get(RegionState.State.SPLIT).size();
      int failedRegionsCount = tableRegions.get(RegionState.State.FAILED_OPEN).size() + tableRegions
          .get(RegionState.State.FAILED_CLOSE).size();
      int otherRegionsCount = 0;
      for (List<HRegionInfo> list : tableRegions.values()) {
        otherRegionsCount += list.size();
      }
      // now subtract known states
      otherRegionsCount =
          otherRegionsCount - openRegionsCount - failedRegionsCount - offlineRegionsCount
              - splitRegionsCount;
    %>
    <tr>
      <td>
        <%= htDesc.getTableName().getNamespaceAsString() %>
      </td>
      <td><a href=table.jsp?name=<%=
      htDesc.getTableName().getNameAsString() %>><%=
      htDesc.getTableName().getQualifierAsString() %>
      </a></td>
      <%if (openRegionsCount <= 1) {%>
      <td><span style="color:red"><%= openRegionsCount %></span></td>
      <% } else { %>
      <td><%= openRegionsCount %>
      </td>
      <%} %>
      <td><%= offlineRegionsCount %>
      </td>
      <td><%= failedRegionsCount %>
      </td>
      <td><%= splitRegionsCount %>
      </td>
      <td><%= otherRegionsCount %>
      </td>
      <td>
          <span class="more">
            <%= htDesc.toString() %>
          </span>
      </td>
    </tr>
    <% } %>
    <p><%= tableLength %> table(s) in set.</p>
    <p>
      <span>Page</span>
      <%
        int pageCount = (int) Math.ceil(tableLength / (double) pageSize);
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
          if (pageNo == pageIndex) {
      %>
      <span><a class="btn disabled" href="#"><%= pageNo %></a></span>
      <% } else { %>
      <span><a class="btn"
               href="/tablesDetailed.jsp?pageIndex=<%= pageNo %>"><%= pageNo %></a></span>
      <% } %>
      <% } %>
    </p>
  </table>
  <% } %>

</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
