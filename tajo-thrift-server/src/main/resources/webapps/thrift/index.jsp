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

<%
  TajoThriftServer tajoThriftServer =
          (TajoThriftServer) InfoHttpServer.getInstance().getAttribute("tajo.thrift.info.server.object");
  ThriftServerContext context = tajoThriftServer.getContext();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo-Proxy</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Thrift Server: <%=context.getServerName()%></h2>
  <hr/>
  <h3>Proxy Server Status</h3>
  <table border='0'>
    <tr><td width='150'>Started:</td><td><%=new Date(context.getStartTime())%></td></tr>
    <tr><td width='150'>Heap(Free/Total/Max): </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB / <%=Runtime.getRuntime().totalMemory()/1024/1024%> MB / <%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
    <tr><td width='150'>Configuration:</td><td><a href='conf.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Environment:</td><td><a href='env.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Threads:</td><td><a href='thread.jsp'>thread dump...</a></tr>
  </table>
  <hr/>
  <h3>Thrift Server Summary</h3>
  <table class="border_table">
    <tr><td witdh='150'># Query Tasks: </td><td width='300'><a href='querytask.jsp'><%=context.getQuerySubmitTasks().size()%></a></td></tr>
    <tr><td witdh='150'># Sessions: </td><td><a href='clientsession.jsp'><%=context.getTajoClientSessions().size()%></a></td></tr>
  </table>
</div>
</body>
</html>
