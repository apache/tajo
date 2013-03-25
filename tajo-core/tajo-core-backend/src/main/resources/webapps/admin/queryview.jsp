<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-- %>

<%@ page import="java.util.*" %>
<%@ page import="tajo.webapp.StaticHttpServer" %>
<%@ page import="nta.engine.*" %>
<%@ page import="nta.engine.cluster.ClusterManager" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="nta.engine.NConstants" %>
<%@ page import="nta.engine.ClientServiceProtos.*" %>
<%@ page import="nta.engine.utils.*" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <link rel="stylesheet" type = "text/css" href = "./style.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>tajo main</title>
    <%
     NtaEngineMaster master = (NtaEngineMaster)application.getAttribute("tajo.master");
     String masterAddr = (String)application.getAttribute("tajo.master.addr");
     List<String> serverList = master.getOnlineServer();
     ClusterManager cm = master.getClusterManager();
     
     String query = request.getParameter("command");
     
     if(query != null) {
       String[] cmd = query.split("\\s|\n");
	   if (cmd[0].equalsIgnoreCase("attach")) {
	     if(cmd.length != 3) {
	     } else {
	       AttachTableRequest.Builder attachRequest = AttachTableRequest.newBuilder();
	       attachRequest.setName(cmd[1]);
	       attachRequest.setPath(cmd[2]);
	       master.attachTable(attachRequest.build());
	     }
	   } else if (cmd[0].equalsIgnoreCase("detach")) {
		 if (cmd.length != 2) {
	     } else {
		  master.detachTable(ProtoUtil.newProto(cmd[1]));
	     }
	   } else {
		 ExecuteQueryRequest.Builder executeRequest = ExecuteQueryRequest.newBuilder();
		 executeRequest.setQuery(query);
	     master.executeQuery(executeRequest.build()); 
	   }
     }
   
    %>
  </head>
  <body>
    <div class = "center">
      <div>
      <img src = "./img/tajochar_queries_small.jpg" />
      </div>
    </div>
    <br />
    <div class = "headline">
      <a href="./catalogview.jsp" class="headline">Catalog</a>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
      <a href="./nodeview.jsp" class="headline">Workers</a>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
      <a href="./index.jsp" class="headline">Summary</a>
    </div>
    <hr />
    <div class = "command" >
      <form method="post" action="./queryview.jsp">
	    <textarea name="command"  class = "command">insert query</textarea>
	    <br />
	    <br />
	    <br />
	    <input type="submit" value="submit" />
      </form>
    </div>
  </body>
</html>
