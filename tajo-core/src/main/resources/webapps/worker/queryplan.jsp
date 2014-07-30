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

<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.*" %>
<%@ page import="org.apache.tajo.master.querymaster.Query" %>
<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryMasterTask" %>
<%@ page import="org.apache.tajo.master.querymaster.SubQuery" %>
<%@ page import="org.apache.tajo.engine.planner.global.ExecutionBlock" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.ExecutionBlockId" %>
<%@ page import="org.apache.tajo.engine.planner.global.MasterPlan" %>
<%@ page import="org.apache.tajo.engine.planner.global.DataChannel" %>

<%
  QueryId queryId = TajoIdUtils.parseQueryId(request.getParameter("queryId"));

  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  QueryMasterTask queryMasterTask = tajoWorker.getWorkerContext()
          .getQueryMasterManagerService().getQueryMaster().getQueryMasterTask(queryId, true);

  if(queryMasterTask == null) {
    out.write("<script type='text/javascript'>alert('no query'); history.back(0); </script>");
    return;
  }

  Query query = queryMasterTask.getQuery();

  Map<ExecutionBlockId, SubQuery> subQueryMap = new HashMap<ExecutionBlockId, SubQuery>();

  for(SubQuery eachSubQuery: query.getSubQueries()) {
    subQueryMap.put(eachSubQuery.getId(), eachSubQuery);
  }

  class SubQueryInfo {
    ExecutionBlock executionBlock;
    SubQuery subQuery;
    ExecutionBlockId parentId;
    int px;
    int py;
    int pos; // 0: mid 1: left 2: right
    public SubQueryInfo(ExecutionBlock executionBlock, SubQuery subQuery, ExecutionBlockId parentId, int px, int py, int pos) {
      this.executionBlock = executionBlock;
      this.subQuery = subQuery;
      this.parentId = parentId;
      this.px = px;
      this.py = py;
      this.pos = pos;
    }
  }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <link rel="stylesheet" type = "text/css" href = "/static/queryplan.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo</title>
  <script type='text/javascript' src='/static/js/jquery.js'></script>
  <script type='text/javascript' src='/static/js/jquery-ui.min.js'></script>
  <script type='text/javascript' src='/static/js/jquery.jsPlumb-1.3.16-all.js'></script>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
  <hr/>
  <div>
    <h3>Distributed Query Execution Plan</h3>
    <div style='float:left'><span class="textborder" style="color:black;font-size:9px">NEW</span></div>
    <div style='float:left;margin-left:5px;'><span class="textborder" style="color:gray;font-size:9px">ALLOCATED</span></div>
    <div style='float:left;margin-left:5px;'><span class="textborder" style="color:skyblue;font-size:9px">INIT</span></div>
    <div style='float:left;margin-left:5px;'><span class="textborder" style="color:blue;font-size:9px">RUNNING</span></div>
    <div style='float:left;margin-left:5px;'><span class="textborder" style="color:green;font-size:9px">SUCCEEDED</span></div>
    <div style='float:left;margin-left:5px;'><span class="textborder" style="color:red;font-size:9px">FAILED</span></div>
  </div>

<!-- draw the query plan -->
<%
  MasterPlan masterPlan = query.getPlan();
  String curIdStr = null;
  int x=35, y=1;
  int pos;
  List<SubQueryInfo> subQueryInfos = new ArrayList<SubQueryInfo>();

  subQueryInfos.add(new SubQueryInfo(masterPlan.getRoot(), null, null, x, y, 0));

  while (!subQueryInfos.isEmpty()) {
    SubQueryInfo eachSubQueryInfo = subQueryInfos.remove(0);
    curIdStr = eachSubQueryInfo.executionBlock.getId().toString();

    y = eachSubQueryInfo.py + 13;
    if (eachSubQueryInfo.pos == 0) {
      x = eachSubQueryInfo.px;
    } else if (eachSubQueryInfo.pos == 1) {
      x = eachSubQueryInfo.px - 20;
    } else if (eachSubQueryInfo.pos == 2) {
      x = eachSubQueryInfo.px + 20;
    }
%>
  <script type='text/javascript'>
    jsPlumb.setRenderMode(jsPlumb.CANVAS);
  </script>

  <div class="component window" id="<%=curIdStr%>" style="left:<%=x%>em;top:<%=y%>em;">
    <a style="font-size:0.9em;" href="./querytasks.jsp?queryId=<%=queryId%>&ebid=<%=curIdStr%>"><%=curIdStr%></a></p>
  </div>

<%
    if (eachSubQueryInfo.parentId != null) {
      String outgoing = "";
      String prefix = "";
      for (DataChannel channel : masterPlan.getOutgoingChannels(eachSubQueryInfo.executionBlock.getId())) {
        outgoing += prefix + channel.getShuffleType();
        prefix = "; ";
      }
%>
  <script type="text/javascript">
    var srcId = "<%=curIdStr%>";
    var destId = "<%=eachSubQueryInfo.parentId.toString()%>";
    var src = window.jsPlumb.addEndpoint(srcId, {
        anchor:"AutoDefault",
        paintStyle:{
          fillStyle:"CornflowerBlue "
        },
        hoverPaintStyle:{
          fillStyle:"red"
        }
      }
    );

    var dst = jsPlumb.addEndpoint(destId, {
        anchor:"AutoDefault",
        paintStyle:{
          fillStyle:"CornflowerBlue "
        },
        hoverPaintStyle:{
          fillStyle:"red"
        }
      }
    );

    var con = jsPlumb.connect({
      source:src,
      target:dst,
      paintStyle:{ strokeStyle:"CornflowerBlue ", lineWidth:3  },
      hoverPaintStyle:{ strokeStyle:"red", lineWidth:4 },
      overlays : [ <!-- overlays start -->
        [ "Arrow", { location:1 } ],
        ["Label", {
          cssClass:"l1 component label",
          label : "<%=outgoing%>",
          location:0.5,
          id:"label",
          events:{
            "click":function(label, evt) {
            }
          }
        }] <!-- label end -->
      ] <!-- overlays end -->
    });
  </script>
<%
    } //end of if
%>

  <script type='text/javascript'>
    var e = document.getElementById("<%=curIdStr%>");
    var state = "<%=eachSubQueryInfo.subQuery != null ? eachSubQueryInfo.subQuery.getState(true).name(): ""%>";
    switch (state) {
      case 'NEW':
        e.style.borderColor = "black";
        e.style.color = "black";
        break;
      case 'CONTAINER_ALLOCATED':
        e.style.borderColor = "gray";
        e.style.color = "gray";
        break;
      case 'INIT':
        e.style.borderColor = "skyblue";
        e.style.color = "skyblue";
        break;
      case 'RUNNING':
        e.style.borderColor = "blue";
        e.style.color = "blue";
        break;
      case 'SUCCEEDED':
        e.style.borderColor = "green";
        e.style.color = "green";
        break;
      case 'FAILED':
        e.style.borderColor = "red";
        e.style.color = "red";
        break;
      default:
        break;
    }
  </script>

<%
    List<ExecutionBlock> children = masterPlan.getChilds(eachSubQueryInfo.executionBlock.getId());

    if (children.size() == 1) {
      pos = 0;
    } else {
      pos = 1;
    }
    for (ExecutionBlock child : children) {
      subQueryInfos.add(new SubQueryInfo(child, subQueryMap.get(child.getId()), eachSubQueryInfo.executionBlock.getId(), x, y, pos++));
    }
  } //end of while
%>

</div>
</body>
</html>