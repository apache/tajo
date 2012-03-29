<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
   
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
  String masterAddr = (String)application.getAttribute("tajo.master.addr");
  masterAddr = masterAddr.split(":")[0];
  String workerName = request.getParameter("workername");
  String fileName = request.getParameter("filename");
  
  com.sun.management.OperatingSystemMXBean bean =
  (com.sun.management.OperatingSystemMXBean)
    java.lang.management.ManagementFactory.getOperatingSystemMXBean();
  long memMax = bean.getTotalPhysicalMemorySize();
  long memFree = bean.getFreePhysicalMemorySize();
  double systemLoadAverage = bean.getSystemLoadAverage();
  String arch = bean.getArch();
  
  
  InetAddress addr = InetAddress.getLocalHost();
  if(workerName == null) {
	  workerName = addr.getHostName();
  }
  String ipAddr = addr.getHostAddress();
  
  File[] roots = File.listRoots();
  long fileMax = 0;
  long fileFree = 0;
  for(int i = 0 ; i < roots.length ; i ++) {
	  fileMax += roots[i].getTotalSpace();
	  fileFree += roots[i].getFreeSpace();
  }
  //String filePath = System.getProperty("tajo.log.dir");
  String filePath = System.getProperty("user.dir")+"/logs";
  
  %>
  <body>
    <div class = "container" >
      <h1>Worker Details</h1>
    </div>
    <div class = "headline_2">
      <div class = "container">
        <a href="http://<%=masterAddr %>:8080/index.jsp" class="headline">Tajo Main</a>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        <a href="http://<%=masterAddr %>:8080/catalogview.jsp" class="headline">Catalog</a>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        <a href="http://<%=masterAddr %>:8080/queryview.jsp" class="headline">Queries</a>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        <a href="http://<%=masterAddr %>:8080/nodeview.jsp" class="headline">Workers</a>
     </div>
    </div>
     <div class = "container">
     <h2 class = "line">Worker Informations</h2>
     <ul>
      <li>Worker Name : <%=workerName%></li>
      <li>Ip Address : <%=ipAddr%></li>
      <li>Architecture : <%=arch%></li>
      <li>Available Processors : <%=bean.getAvailableProcessors()%></li>
      <li>OS : <%=bean.getName()%>
      <li>System Load Average : <%=systemLoadAverage%></li>
      <li>Disk Num : <%=roots.length %></li>
      <li>Disk <ul>
                <li>Max Size : <%=fileMax%> Byte</li>
                <li>Free Size : <%=fileFree%> Byte</li>
               </ul>
               </li>
      <li>Memory <ul>
                 <li>Max Size : <%=memMax%> Byte</li>
                 <li>Free Size : <%=memFree%> Byte</li>
                 </ul>
     </ul>     
     
     <h2 class = "line">Log List</h2>
      <%
      File file = new File(filePath);
      String[] fileList = file.list();
      out.write("<ul>");
      for(int i = 0 ; i < fileList.length; i ++){
    	out.write("<li><a href=\"./nodedetail.jsp?workername="+workerName+"&filename="+fileList[i]+"\" class = \"tablelink\"> "+ fileList[i] + "</a>"); 
      } 
      out.write("</ul>");
      %>
     
     
     <h2 class = "line">Log</h2>
     <textarea id = "logview"  readonly = "readonly" style="width:99%; height:600px">
      <% 
      if(fileName != null) {
       String str = ""; 
       for( int i = 0 ; i < fileList.length ; i ++) {
    	  if(fileName.compareTo(fileList[i]) != 0) {
    		  continue;
    	  }
    	  reader = new BufferedReader(new InputStreamReader(
    	     		  new FileInputStream(filePath +"/"  + fileList[i])));
      %>
<%=filePath+"/"+fileList[i] %>
          
          <%
out.write("-------------------------------------------------------------------\n");   
    	  while((str = reader.readLine()) != null) {
	        out.write(str + "\n" );
    	  }
          
       }
       reader.close();
      }
       %>
       </textarea>
       <br />
       <br />
       <br />
       <br />
     </div>
  </body>
</html>
