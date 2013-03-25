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
<%@ page import="java.net.InetAddress" %>
<%@ page import = "java.io.*" %>
<%@ page import="tajo.webapp.StaticHttpServer" %>
<%@ page import="nta.engine.*" %>
<%@ page import="nta.engine.cluster.ClusterManager" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="nta.engine.NConstants" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <link rel="stylesheet" type = "text/css" href = "./style.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>tajo</title>
  </head>
  <%!
  BufferedReader reader;
  %>
  <%
  NtaEngineMaster master = (NtaEngineMaster)application.getAttribute("tajo.master");
  String masterAddr = (String)application.getAttribute("tajo.master.addr");
  List<String> serverList = master.getOnlineServer();
  masterAddr = masterAddr.split(":")[0];
  
  %>
  <body>
    <div class = "container" >
      <div>
      <img src = "./img/tajochar_worker_small.jpg" />
      </div>
    </div>
    <br />
    <div class = "headline_2">
      <div class = "container">
        <a href="./index.jsp" class="headline">Tajo Main</a>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        <a href="./catalogview.jsp" class="headline">Catalog</a>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        <a href="./queryview.jsp" class="headline">Queries</a>
     </div>
    </div>
    
    <div class ="container">
    <h2 class = "line">Available workers</h2>
    <ul>
     <%
      for(int i = 0 ; i < serverList.size() ; i ++ ) {
    	String workerName = serverList.get(i);  
    	out.write("<li><a href =\"http://" +workerName.split(":")[0] + ":8080/nodedetail.jsp?workername="+workerName+"\" class = \"tablelink\">" + workerName + "</a></li>");
      }
     %>
    </ul>
    </div>
    
 
  </body>
</html>
