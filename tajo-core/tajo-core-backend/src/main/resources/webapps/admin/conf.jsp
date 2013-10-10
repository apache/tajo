<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.conf.*" %>
<%@ page import="java.util.Map" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  TajoMaster.MasterContext context = master.getContext();
  TajoConf tajoConf = context.getConf();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=master.getMasterName()%></h2>
  <hr/>
  <table width="100%" border="1" class="border_table">
<%
  for(Map.Entry<String,String> entry: tajoConf) {
%>
    <tr><td width="200"><%=entry.getKey()%></td><td><%=entry.getValue()%></td>
<%
  }
%>
  </table>
</div>
</body>
</html>