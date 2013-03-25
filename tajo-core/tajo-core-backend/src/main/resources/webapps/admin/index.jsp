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
<%@ page import="nta.catalog.*" %>
<%@ page import="nta.engine.*" %>
<%@ page import="nta.engine.cluster.ClusterManager" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="nta.engine.NConstants" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <link rel="stylesheet" type = "text/css" href = "./style.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>tajo main</title>
    <%
     NtaEngineMaster master = (NtaEngineMaster)application.getAttribute("tajo.master");
     CatalogService catalog = master.getCatalog(); 
     HashMap<String,String> map = (HashMap<String,String>)application.getAttribute("tajo.online.worker");
     List<String> serverList = master.getOnlineServer();
     HashMap<String,String> curMap = new HashMap<String, String>();
     if(map == null) {
       map = new HashMap<String,String>();
       for(int i = 0 ; i < serverList.size() ; i ++) {
    	   String tmp = serverList.get(i);
    	   map.put(tmp,tmp);
    	   curMap.put(tmp, tmp);
       }
       application.setAttribute("tajo.online.worker", map);
     }else {
       for(int i = 0 ; i < serverList.size() ; i ++) {
    	   String tmp = serverList.get(i);
    	   if(!map.containsKey(tmp)) {
    		   map.put(tmp, tmp);
    	   }
    	   curMap.put(tmp, tmp);
       }
     }
     String masterAddr = (String)application.getAttribute("tajo.master.addr");
     ClusterManager cm = master.getClusterManager();
     String[] tableArr = catalog.getAllTableNames().toArray(new String[0]);
     
     long totalDisk = 0;
     long availableDisk = 0;
     final double MB = Math.pow(1024, 4);
     for(int i = 0 ; i < serverList.size() ; i ++) {
      ClusterManager.WorkerInfo info = cm.getWorkerInfo(serverList.get(i));
      List<ClusterManager.DiskInfo> disks = info.disks;
      for(int j = 0 ; j < disks.size() ; j ++) {
    	  totalDisk += disks.get(j).totalSpace;
    	  availableDisk += disks.get(j).freeSpace;
      }
     }
     
     long time = (System.currentTimeMillis() - (Long)application.getAttribute("tajo.master.starttime"))/1000;
     
    %>
  </head>
  <body>
    <div class = "container">
      <div>
      <img src="./img/tajochar_title_small.jpg" />
      </div>
    </div>
    <br />
    <div class = "headline_2">
     <div class = "container">
      <a href="./catalogview.jsp" class="headline">Catalog</a>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
      <a href="./nodeview.jsp" class="headline">Workers</a>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
      <a href="./queryview.jsp" class="headline">Queries</a>
     </div>
    </div>
    
    <div class ="container">
     <div class = "outbox">
      <h2 class = "compactline">System Summary</h2>
      <table align = "center"class = "noborder">
       <tr>
        <th class="rightbottom">Cluster Number</th>
        <th class="rightbottom">Live workers</th>
        <th class="rightbottom">Table number</th>
        <th class="rightbottom">Total Disk Size</th>
        <th class="rightbottom">Available Disk Size</th>
        <th class="bottom">Running Time</th>
       </tr>
       <tr>
        <td class="rightborder"><%=map.size()%></td>
        <td class="rightborder"><%=curMap.size()%></td>
        <td class="rightborder"><%=tableArr.length%></td>
        <td class="rightborder"><%=String.format("%.2f", totalDisk/MB)%>TB</td>
        <td class="rightborder"><%=String.format("%.2f", availableDisk/MB)%>TB</td>
        <td class="noborder"><%=time/3600/24 + "d" + time/3600 + "h" + time/60%60 + "m" + time%60+"s"%></td>
       </tr>
      </table>
     </div>
    
     <div class="outbox_order">
      <h2 class="compactline">Table List</h2>     
      <table align = "center" class = "new">
      <tr>
       <th style="width:110px">TableName</th>
       <th>TablePath</th>
      </tr>
      <%
      for(int i = 0 ; i < tableArr.length ; i ++ ) {
        TableDesc table = catalog.getTableDesc(tableArr[i]);    
      %>
      <tr>
        <td><a href = "./catalogview.jsp?tablename=<%=table.getId()%>" class = "tablelink"><%=table.getId()%></a></td>
        <td><%=table.getPath()%></td>
      </tr>	
      <% 	
      }
      %>
      </table>
     </div>
     <div style="float:left; width:6px">&nbsp;</div>
     <div class="outbox_order">
      <h2 class="compactline">Worker List</h2>
      <table align="center" class = "new">
      <tr>
        <th style="width:90px">Status</th>
        <th>Worker Name</th>
      </tr>
      <%
      Set<String> keySet = map.keySet();
      for(String key : keySet ) {
    	out.write("<tr>");  
     	if(!curMap.containsKey(key)) {
     	  out.write("<td><img src=\"./img/off.jpg\" />&nbsp;&nbsp;  Offline</td>");
     	  out.write("<td><a href =\"http://" +key.split(":")[0] + ":8080/nodedetail.jsp?workername="+key+"\" class = \"tablelink\">" + key + "</a></td>");
     	}
     	else{
     	  out.write("<td><img src=\"./img/on.jpg\" />&nbsp;&nbsp;  Online</td>");
       	  out.write("<td><a href =\"http://" +key.split(":")[0] + ":8080/nodedetail.jsp?workername="+key+"\" class = \"tablelink\">" + key + "</a></td>");
       	}
     	out.write("</tr>");
       }
     %>
     </table> 
    </div>
    
    <div style="clear:both"></div>
    
   
  </body>
</html>
