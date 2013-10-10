<%@ page import="org.apache.tajo.util.JSPUtil" %>
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

