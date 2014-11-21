<%
  /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.thrift.*" %>
<%@ page import="org.apache.tajo.thrift.TajoThriftServer.*" %>
<%@ page import="java.util.*" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="org.apache.tajo.util.StringUtils" %>

<%
  TajoThriftServer tajoThriftServer =
      (TajoThriftServer) InfoHttpServer.getInstance().getAttribute("tajo.thrift.info.server.object");
  ThriftServerContext context = tajoThriftServer.getContext();
  Collection<QueryProgressInfo> queryTasks = context.getQuerySubmitTasks();

  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Tajo-Proxy</title>
    <script src="/static/js/jquery.js" type="text/javascript"></script>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Thrift Server: <%=context.getServerName()%></h2>
  <hr/>
  <h3>Running Query Tasks</h3>
<%
  if(queryTasks.isEmpty()) {
    out.write("No running query tasks.");
  } else {
%>
  <div># Tasks: <%=queryTasks.size()%></div>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>SessionId</th><th>Started</th><th>Progress</th><th>Time</th><th>Status</th><th>Last Touch Time</th></th><th>sql</th></tr>
    <%
      for(QueryProgressInfo eachQuery: queryTasks) {
        long time = System.currentTimeMillis() - eachQuery.getQueryStatus().getSubmitTime();
    %>
    <tr>
      <td><%=eachQuery.getQueryId()%></td>
      <td><%=eachQuery.getSessionId().getId()%></td>
      <td><%=df.format(eachQuery.getQueryStatus().getSubmitTime())%></td>
      <td><%=(int)(eachQuery.getQueryStatus().getProgress() * 100.0f)%>%</td>
      <td><%=StringUtils.formatTime(time)%></td>
      <td><%=eachQuery.getQueryStatus().getState()%></td>
      <td><%=df.format(eachQuery.getLastTouchTime())%></td>
      <td><%=eachQuery.getQuery()%></td>
    </tr>
    <%
      }
    %>
  </table>
<%
  }
%>
</div>
</body>
</html>
