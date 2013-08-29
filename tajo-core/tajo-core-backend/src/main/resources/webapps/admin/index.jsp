<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="java.net.InetSocketAddress" %>
<%@ page import="java.net.InetAddress"  %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.master.*" %>
<%@ page import="org.apache.tajo.master.rm.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>

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
<table>
  <tr><th>Worker</th><th>Ports</th><th>Slot</th></th><th>Memory(Used/Capacity)</th><th>Disk</th><th>Cpu</th><th>Status</th></tr>
  <%
    List<String> wokerKeys = new ArrayList<String>(workers.keySet());
    Collections.sort(wokerKeys);
    for(String eachWorker: wokerKeys) {
      WorkerResource worker = workers.get(eachWorker);
  %>
  <tr>
    <td><%=eachWorker%></td>
    <td><%=worker.portsToStr()%></td>
    <td><%=worker.getUsedSlots()%>/<%=worker.getSlots()%></td>
    <td><%=worker.getUsedMemoryMBSlots()%>/<%=worker.getMemoryMBSlots()%> MB</td>
    <td><%=worker.getUsedDiskSlots()%>/<%=worker.getDiskSlots()%></td>
    <td><%=worker.getUsedCpuCoreSlots()%>/<%=worker.getCpuCoreSlots()%></td>
    <td><%=worker.getWorkerStatus()%></td>
  </tr>
  <%
    }

    if(workers.isEmpty()) {
  %>
  <tr>
    <td colspan='7'>No Workers</td>
  </tr>
  <%
    }
  %>
</table>
</body>
</html>
