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
         import="static org.apache.commons.lang3.StringEscapeUtils.escapeXml"
         import="java.io.IOException"
         import="java.util.ArrayList"
         import="java.util.List"
         import="java.util.Map"
%>
<%@ page import="org.apache.hadoop.hbase.client.TableDescriptor" %>
<%@ page import="org.apache.hadoop.hbase.client.TableState" %>
<%@ page import="org.apache.hadoop.hbase.client.RegionInfo" %>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.tmpl.master.MasterStatusTmplImpl" %>
<%@ page import="org.apache.hadoop.hbase.HTableDescriptor" %>
<%@ page import="org.apache.hadoop.hbase.TableName" %>
<%@ page import="org.apache.hadoop.hbase.master.RegionState" %>
<%@ page import="com.xiaomi.infra.thirdparty.com.google.common.base.Strings" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master: " + master.getServerName());
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>User Tables</h1>
    </div>
  </div>

  <% 
    List<TableDescriptor> tables = new ArrayList<TableDescriptor>();
    String errorMessage = MasterStatusTmplImpl.getUserTables(master, tables);
    String pageIndexStr = request.getParameter("pageIndex");
    String pageSizeStr = request.getParameter("pageSize");
    int pageIndex = Strings.isNullOrEmpty(pageIndexStr) ? 1 : Integer.parseInt(pageIndexStr);
    int pageSize = Strings.isNullOrEmpty(pageSizeStr) ? 100 : Integer.parseInt(pageSizeStr);
    int tableLength = 0;
    if (tables.size() == 0 && errorMessage != null) { %>
  <p> <%= errorMessage %> </p>
  <%
    } else {
      tableLength = tables.size();
      int startIndex = (pageIndex - 1) * pageSize;
      int endIndex = Math.min(tables.size(), startIndex + pageSize);
      tables = tables.subList(startIndex, endIndex);
    }
  if (tableLength > 0) { %>
  <table class="table table-striped">
    <thead>
      <tr>
        <th style="vertical-align: middle;" rowspan="2">Namespace</th>
        <th style="vertical-align: middle;" rowspan="2">Name</th>
        <th style="vertical-align:middle;" rowspan="2">State</th>
        <th style="text-align: center" colspan="8">Regions</th>
        <th style="vertical-align:middle;" rowspan="2">Description</th>
      </tr>
      <tr>
        <th>OPEN</th>
        <th>OPENING</th>
        <th>CLOSED</th>
        <th>CLOSING</th>
        <th>OFFLINE</th>
        <th>FAILED</th>
        <th>SPLIT</th>
        <th>Other</th>
      </tr>
    </thead>
    <tbody>
    <%for (TableDescriptor desc : tables) {%>
    <%
      HTableDescriptor htDesc = new HTableDescriptor(desc);
      TableName tableName = htDesc.getTableName();
      TableState tableState = master.getTableStateManager().getTableState(tableName);
      Map<RegionState.State, List<RegionInfo>> tableRegions =
          master.getAssignmentManager().getRegionStates()
            .getRegionByStateOfTable(tableName);
      int openRegionsCount = tableRegions.get(RegionState.State.OPEN).size();
      int openingRegionsCount = tableRegions.get(RegionState.State.OPENING).size();
      int closedRegionsCount = tableRegions.get(RegionState.State.CLOSED).size();
      int closingRegionsCount = tableRegions.get(RegionState.State.CLOSING).size();
      int offlineRegionsCount = tableRegions.get(RegionState.State.OFFLINE).size();
      int splitRegionsCount = tableRegions.get(RegionState.State.SPLIT).size();
      int failedRegionsCount = tableRegions.get(RegionState.State.FAILED_OPEN).size()
             + tableRegions.get(RegionState.State.FAILED_CLOSE).size();
      int otherRegionsCount = 0;
      for (List<RegionInfo> list: tableRegions.values()) {
         otherRegionsCount += list.size();
      }
      // now subtract known states
      otherRegionsCount = otherRegionsCount - openRegionsCount
                     - failedRegionsCount - offlineRegionsCount
                     - splitRegionsCount - openingRegionsCount
                     - closedRegionsCount - closingRegionsCount;
    %>
    <tr>
        <td>
          <%= htDesc.getTableName().getNamespaceAsString() %>
        </td>
        <td>
          <a href=table.jsp?name=<%=
          htDesc.getTableName().getNameAsString() %>><%=
          htDesc.getTableName().getQualifierAsString() %>
          </a>
        </td>
        <td><%= tableState.getState() %></td>
        <%if (openRegionsCount <= 1) {%>
        <td><span style="color:red"><%= openRegionsCount %></span></td>
        <% } else { %>
        <td><%= openRegionsCount %></td>
        <% } %>
        <%if (openingRegionsCount > 1) {%>
        <td><a href="/rits.jsp?table=<%= tableName.getNameAsString() %>&state=OPENING"><%= openingRegionsCount %></td>
        <% } else { %>
        <td><%= openingRegionsCount %></td>
        <% } %>
        <td><%= closedRegionsCount %></td>
        <%if (closingRegionsCount > 1) {%>
        <td><a href="/rits.jsp?table=<%= tableName.getNameAsString() %>&state=CLOSING"><%= closingRegionsCount %></td>
        <% } else { %>
        <td><%= closingRegionsCount %></td>
        <% } %>
        <td><%= offlineRegionsCount %></td>
        <td><%= failedRegionsCount %></td>
        <td><%= splitRegionsCount %></td>
        <td><%= otherRegionsCount %></td>
        <td><%= htDesc.toStringCustomizedValues() %></td>
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
    </tbody>
  </table>
  <% } %>
</div>

<jsp:include page="footer.jsp"/>
