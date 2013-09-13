<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryInProgress" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "./style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>tajo main</title>
  <%
    TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
    CatalogService catalog = master.getCatalog();
    Map<String, WorkerResource> workers = master.getContext().getResourceManager().getWorkers();
  %>
</head>
<body>
<img src='img/tajo_logo.png'/>
<hr/>

<a href="index.jsp">Main</a>
<a href="query.jsp">Query</a>

<div><h3>Query</h3></div>
<div>
  <textarea></textarea>
</div>
<div>Run Query</div>
</body>
</html>