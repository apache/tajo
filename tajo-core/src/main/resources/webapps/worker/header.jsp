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
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%
  TajoWorker tmpTajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  String tajoMasterHttp = "http://" + JSPUtil.getTajoMasterHttpAddr(tmpTajoWorker.getConfig());
%>
<div class="menu">
  <div style='float:left; margin-left:12px; margin-top:6px;'><a href='<%=tajoMasterHttp%>/index.jsp'><img src='/static/img/logo_tajo.gif' border='0'/></a></div>
  <ul>
    <li><a class='top_menu_item' style='margin-left:10px;' href='<%=tajoMasterHttp%>/index.jsp'>Home</a></li>
    <li><a class='top_menu_item' href='<%=tajoMasterHttp%>/cluster.jsp'>Cluster</a></li>
    <li><a class='top_menu_item' href='<%=tajoMasterHttp%>/query.jsp'>Query</a></li>
    <li><a class='top_menu_item' href='<%=tajoMasterHttp%>/catalogview.jsp'>Catalog</a></li>
    <li><a class='top_menu_item' href='<%=tajoMasterHttp%>/query_executor.jsp'>Execute Query</a></li>
  </ul>
  <br style="clear: left" />
</div>

