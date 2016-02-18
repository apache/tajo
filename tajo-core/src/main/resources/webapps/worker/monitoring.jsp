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
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.net.InetSocketAddress" %>
<%
  TajoWorker node = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  String[] nodeName = node.getWorkerContext().getWorkerName().split(":");
  InetSocketAddress socketAddress = new InetSocketAddress(nodeName[0], Integer.parseInt(nodeName[1]));
  String nodeLabel = socketAddress.getAddress().getHostName()+ ":" + socketAddress.getPort();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<link rel="stylesheet" type = "text/css" href="/static/style.css" />
<link rel="stylesheet" type="text/css" href="/static/nv.d3.min.css" />
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Tajo</title>
<script src="/static/js/jquery.js" type="text/javascript"></script>
<script src="/static/js/d3.min.js" type="text/javascript" charset="utf-8"></script>
<script src="/static/js/nv.d3.min.js" type="text/javascript"></script>
<script src="/static/js/moment.js" type="text/javascript"></script>
<style>
  div.group {
  }
  div.group > div.chart {
    display:inline-block;
  }
  div.chart > svg {
    display: block;
    height: 280px !important;
    width: 400px !important;
  }

  div.group_summary > .positionLeft {
    float: left;
  }
  div.group_summary > .positionRight {
    text-align: right;
  }
  div.group_summary > .positionBottom {
    clear: both;
  }
</style>
<script type="text/javascript">
(function ($) {
  $.tajoMonitor = {
    TYPE: {
      VALUE: 1,
      PERCENT: 2,
      BYTE: 3,
      KBYTE: 4,
      MBYTE: 5
    },
    KEY_PREFIX: '<%=nodeLabel%>',
    KEY_DELIM: '-',
    BUF_SIZE: 30,
    TICK_TIME: 1000 * 10,
    getKey: function(metric, preKey){
      if(preKey){
        return (preKey + this.KEY_DELIM + metric).toLowerCase();
      }
      return (this.KEY_PREFIX + this.KEY_DELIM + metric).toLowerCase();
    },
    getSeri: function(obj){
      return JSON.stringify(obj);
    },
    getDeseri: function(str){
      return JSON.parse(str);
    },
    offerLocalStorage: function(metric, json, preKey){
      var keyMetric = this.getKey(metric, preKey);
      if(!localStorage.getItem(keyMetric)){
        localStorage.setItem(keyMetric, this.getSeri(new Array()));
      }
      var arr = this.getDeseri(localStorage.getItem(keyMetric));
      if(arr.length>=this.BUF_SIZE){
        arr.shift();
      }
      arr.push(json);
      localStorage.setItem(keyMetric, this.getSeri(arr));
    },
    putLocalStorage: function(metric, json, preKey){
      var keyMetric = this.getKey(metric, preKey);
      localStorage.setItem(keyMetric, this.getSeri(json));
    },
    getLocalStorage: function(metric, preKey){
      var keyMetric = this.getKey(metric, preKey);
      return this.getDeseri(localStorage.getItem(keyMetric));
    },
    clearLocalStorage: function(metric, preKey){
      var keyMetric = this.getKey(metric, preKey);
      localStorage.setItem(keyMetric, this.getSeri(new Array()));
    },
    removeLocalStorage: function(metric, preKey){
      var keyMetric = this.getKey(metric, preKey);
      localStorage.removeItem(keyMetric);
    },
    getAjaxMetaData : function(callback, hostInfo){
      var protocol = location.protocol;
      $.ajax({
        url: (hostInfo)?protocol+'//'+hostInfo+'/metrics':'./metrics',
        data: { action: 'getMetrics' },
        dataType:'json',
        success:function(data){
          callback(data);
        }
      });
    }
  };
  $.tajoMonitor.util = {
    KEY_AUTO_REFRESH: 'key-auto-refresh',
    KEY_REFRESH_INTERVAL: 'key-refresh-interval',
    KEY_SELECTED_NODE: 'key-selected-node',
    getAutoRefresh: function(){
      return $.tajoMonitor.getLocalStorage($.tajoMonitor.util.KEY_AUTO_REFRESH, $.tajoMonitor.KEY_PREFIX);
    },
    putAutoRefresh: function(value){
      $.tajoMonitor.putLocalStorage($.tajoMonitor.util.KEY_AUTO_REFRESH, value, $.tajoMonitor.KEY_PREFIX);
    },
    getRefreshInterval: function(){
      return $.tajoMonitor.getLocalStorage($.tajoMonitor.util.KEY_REFRESH_INTERVAL, $.tajoMonitor.KEY_PREFIX);
    },
    putRefreshInterval: function(value){
      $.tajoMonitor.putLocalStorage($.tajoMonitor.util.KEY_REFRESH_INTERVAL, value, $.tajoMonitor.KEY_PREFIX);
    },
    getSelectedNode: function(){
      return $.tajoMonitor.getLocalStorage($.tajoMonitor.util.KEY_SELECTED_NODE, $.tajoMonitor.KEY_PREFIX);
    },
    putSelectedNode: function(value){
      $.tajoMonitor.putLocalStorage($.tajoMonitor.util.KEY_SELECTED_NODE, value, $.tajoMonitor.KEY_PREFIX);
    }
  };
  $.tajoMonitor.parser = {
    parseSharedPie: function(meta, v, k){
      var makeValue = function(key, value){
        return {key: key, value: value};
      };
      var makeChartData = function(_k, _v){
        var result = new Array();
        for (i = 0; i < _k.length; i++) {
          result.push(makeValue(_k[i], _v[i]));
        }
        return result;
      };
      if(meta.success){
        var _k = [];
        var _v = [];
        var _v2 = [];
        $.each(v, function(index, value) {
          try{
            _v.push(meta.metrics[v[index]].value);
            if(k){
              _k.push(k[index]);
            } else {
              _v2.push(v[index]);
            }
          }catch(e){}
        });
        if(k){
          return makeChartData(_k, _v);
        } else {
          return makeChartData(_v2, _v);
        }
      } else {
        return null;
      }
    },
    parseWorkerClusterUptime: function(meta){
      if(meta.success){
        try{
          return parseInt(meta.metrics['WORKER.CLUSTER.UPTIME'].value/1000);
        }catch(e){}
      }
      return null;
    },
    parseSharedUsed: function(meta, k, type, preKey){
      var makeValue = function(k, v){
        return {x: k, y: v};
      };
      var makeChartData = function(_k, _preKey){
        var result = new Array();
        for (i = 0; i < _k.length; i++) {
          var $v = $.tajoMonitor.getLocalStorage(_k[i], _preKey);
          var item = {key: k[i], values: $v};
          result.push(item);
        }
        return result;
      };
      if(meta.success){
        var _k = [];
        var _v = [];
        for (i = 0; i < k.length; i++) {
          try{
            var _val = 0;
            if(type == $.tajoMonitor.TYPE.PERCENT){
              _val = parseInt(meta.metrics[k[i]].value * 100);
            } else {
              _val = meta.metrics[k[i]].value;
            }
            _v.push(_val);
            _k.push(k[i]);
          }catch(e){}
        }
        var timestamp = meta.timestamp;
        $.each(_k, function(index, value) {
          $.tajoMonitor.offerLocalStorage(_k[index], makeValue(timestamp, _v[index]), preKey);
        });
        return makeChartData(_k, preKey);
      } else {
        return null;
      }
    },
    parseSharedUsage: function(meta, k){
      var makeValue = function(key, value){
        return {key: key, y: value};
      };
      var makeChartData = function(_k, _v){
        var result = new Array();
        for (i = 0; i < _k.length; i++) {
          result.push(makeValue(_k[i], _v[i]));
        }
        return result;
      };
      if(meta.success){
        var _k = [];
        var timestamp = meta.timestamp;
        try{
          var v = parseInt(meta.metrics[k[0]].value*100);
          _k.push(k[0]);
          if(k.length>1){
            _k.push(k[1]);
          }
          var idle = parseInt(100-v);
              v = [v, idle];
          return makeChartData(_k, v);
        }catch(e){}
      }
      return null;
    }
  };
  $.tajoMonitor.chart = {
    chartSharedPie: [],
    loadSharedPie: function(selector, data){
      try{
        if(!$.tajoMonitor.chart.loadSharedPie[selector]){
          nv.addGraph(function () {
            $.tajoMonitor.chart.loadSharedPie[selector] = nv.models.pieChart()
                .x(function (d) { return d.key })
                .y(function (d) { return d.value })
                .donut(true)
                .showLabels(true)
                .labelsOutside(true)
                .labelType('key')
                .valueFormat(d3.format(',.0f'));
              d3.select(selector)
                .datum(data)
                .transition().duration(1200)
                .call($.tajoMonitor.chart.loadSharedPie[selector]);
          });
        } else {
          d3.select(selector)
            .datum(data);
          $.tajoMonitor.chart.loadSharedPie[selector].update();
        }
        return $.tajoMonitor.chart.loadSharedPie[selector];
      } catch(e) {
        console.log('Oops sorry, something wrong with data');
        console.log(e.description);
      }
    },
    chartWorkerClusterUptime: null,
    loadWorkerClusterUptime: function(selector, data){
      try{
        if($.tajoMonitor.chart.chartWorkerClusterUptime){
          $.tajoMonitor.chart.chartWorkerClusterUptime.clearInterval();
        }
        $.tajoMonitor.chart.chartWorkerClusterUptime = new Moment(data, function(obj){
          $(selector).html(obj.toString());
        });
        $(selector).html($.tajoMonitor.chart.chartWorkerClusterUptime.toString());
      } catch(e) {
        console.log('Oops sorry, something wrong with data');
        console.log(e.description);
      }
    },
    chartSharedLine: [],
    loadSharedLine: function(selector, data, type){
      try{
        if(!$.tajoMonitor.chart.chartSharedLine[selector]){
          nv.addGraph(function() {
            $.tajoMonitor.chart.chartSharedLine[selector] = nv.models.lineChart()
                  .margin({left: 100})
                  .useInteractiveGuideline(true)
                  .showLegend(true)
                  .showYAxis(true)
                  .showXAxis(true);
            $.tajoMonitor.chart.chartSharedLine[selector].xAxis
                .axisLabel('Time')
                .tickFormat(function(d) { return d3.time.format('%X')(new Date(d))});
            $.tajoMonitor.chart.chartSharedLine[selector].yAxis
                .tickFormat(function(d) {
                  if(type==$.tajoMonitor.TYPE.KBYTE){
                    return d3.format(',.0f')(d/1024);
                  } else if(type==$.tajoMonitor.TYPE.MBYTE){
                    return d3.format(',.0f')(d/1024/1024);
                  } else {
                    return d3.format(',.0f')(d);
                  }
                });
            var typeStr = null;
            if(type){
              if(type==$.tajoMonitor.TYPE.PERCENT){
                typeStr = '%';
              } else if(type==$.tajoMonitor.TYPE.BYTE) {
                typeStr = 'Bytes';
              } else if(type==$.tajoMonitor.TYPE.KBYTE) {
                typeStr = 'KBytes';
              } else if(type==$.tajoMonitor.TYPE.MBYTE) {
                typeStr = 'MBytes';
              }
            }
            if(typeStr){
              $.tajoMonitor.chart.chartSharedLine[selector].yAxis.axisLabel(typeStr);
            }
            d3.select(selector)
              .datum(data)
              .call($.tajoMonitor.chart.chartSharedLine[selector]);
            $.tajoMonitor.chart.chartSharedLine[selector].update();
          });
        } else {
          d3.select(selector)
            .datum(data)
            .call($.tajoMonitor.chart.chartSharedLine[selector]);
          $.tajoMonitor.chart.chartSharedLine[selector].update();
        }
        return $.tajoMonitor.chart.chartSharedLine[selector];
      } catch(e) {
        console.log('Oops sorry, something wrong with data');
        console.log(e.description);
      }
    },
    chartSharedUsage: [],
    loadSharedUsage: function(selector, data){
      if(data && data.length>0){
        try{
          if(data[0].y<0 || (data.length>1 && data[1].y<0)){
            $(selector).remove();
            return;
          }
        }catch(e){}
      } else if(!data){
        $(selector).remove();
        return;
      }
      try{
        if(!$.tajoMonitor.chart.chartSharedUsage[selector]){
          nv.addGraph(function () {
            var arcRadius1 = [
              { inner: 0.6, outer: 1 },
              { inner: 0.65, outer: 0.95 }
            ];
            $.tajoMonitor.chart.chartSharedUsage[selector] = nv.models.pieChart()
              .x(function (d) { return d.key })
              .y(function (d) { return d.y })
              .donut(true)
              .showLabels(false)
              .growOnHover(false)
              .arcsRadius(arcRadius1)
              .valueFormat(d3.format(',.0f'));
            $.tajoMonitor.chart.chartSharedUsage[selector].title("0%");
            d3.select(selector)
              .datum(data)
              .transition().duration(1200)
              .call($.tajoMonitor.chart.chartSharedUsage[selector]);
            $.tajoMonitor.chart.chartSharedUsage[selector].title(data[0].y + "%");
            $.tajoMonitor.chart.chartSharedUsage[selector].update();
          });
        } else {
          d3.select(selector)
            .datum(data);
          $.tajoMonitor.chart.chartSharedUsage[selector].title(data[0].y + "%");
          $.tajoMonitor.chart.chartSharedUsage[selector].update();
        }
        return $.tajoMonitor.chart.chartSharedUsage[selector];
      } catch(e) {
        console.log('Oops sorry, something wrong with data');
        console.log(e.description);
      }
    }
  }
})(jQuery);
</script>
<script type="text/javascript">
var nodeInterval = null;
var autoRefreshInterval = null;
$(document).ready(function() {
  if(typeof(Storage)!=="undefined"){
    var getAjaxMetaData = function(){
      $.tajoMonitor.getAjaxMetaData(function(data){
        var chartNodeData = [$.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.FILE', 'idle']),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.GC.PS-MarkSweep.count', 'NODE-JVM.GC.PS-Scavenge.count'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.GC.PS-MarkSweep.time', 'NODE-JVM.GC.PS-Scavenge.time'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.LOG.Error', 'NODE-JVM.LOG.Fatal',
                                                      'NODE-JVM.LOG.Info', 'NODE-JVM.LOG.Warn'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.MEMORY.heap.committed', 'NODE-JVM.MEMORY.heap.init',
                                                      'NODE-JVM.MEMORY.heap.max', 'NODE-JVM.MEMORY.heap.used'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.heap.usage', 'idle']),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.MEMORY.non-heap.committed', 'NODE-JVM.MEMORY.non-heap.init',
                                                      'NODE-JVM.MEMORY.non-heap.max', 'NODE-JVM.MEMORY.non-heap.used'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.non-heap.usage', 'idle']),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.pools.Code-Cache.usage', 'idle'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.pools.PS-Eden-Space.usage', 'idle'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.pools.PS-Old-Gen.usage', 'idle'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.pools.PS-Perm-Gen.usage', 'idle'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsage(data, ['NODE-JVM.MEMORY.pools.PS-Survivor-Space.usage', 'idle'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.MEMORY.total.committed', 'NODE-JVM.MEMORY.total.init',
                                                      'NODE-JVM.MEMORY.total.max', 'NODE-JVM.MEMORY.total.used'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.THREAD.count', 'NODE-JVM.THREAD.daemon.count',
                                                      'NODE-JVM.THREAD.new.count', 'NODE-JVM.THREAD.runnable.count'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE-JVM.THREAD.timed_waiting.count', 'NODE-JVM.THREAD.waiting.count',
                                                      'NODE-JVM.THREAD.blocked.count', 'NODE-JVM.THREAD.deadlock.count',
                                                      'NODE-JVM.THREAD.terminated.count'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE.QUERYMASTER.RUNNING_QM'], $.tajoMonitor.TYPE.VALUE),
          $.tajoMonitor.parser.parseSharedUsed(data, ['NODE.TASKS.RUNNING_TASKS'], $.tajoMonitor.TYPE.VALUE)];

        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmFile svg', chartNodeData[0]);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmGcCount svg', chartNodeData[1]);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmGcTime svg', chartNodeData[2]);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmLog svg', chartNodeData[3]);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmMemoryHeap svg', chartNodeData[4], $.tajoMonitor.TYPE.BYTE);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryHeapUsage svg', chartNodeData[5], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmMemoryNonHeap svg', chartNodeData[6], $.tajoMonitor.TYPE.BYTE);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryNonHeapUsage svg', chartNodeData[7], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryPoolsCodeCacheUsage svg', chartNodeData[8], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryPoolsPSEdenSpaceUsage svg', chartNodeData[9], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryPoolsPSOldGenUsage svg', chartNodeData[10], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryPoolsPSPermGenUsage svg', chartNodeData[11], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedUsage('#nodeJvmMemoryPoolsPSSurvivorSpaceUsage svg', chartNodeData[12], $.tajoMonitor.TYPE.PERCENT);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmMemoryTotal svg', chartNodeData[13], $.tajoMonitor.TYPE.BYTE);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmThread svg', chartNodeData[14]);
        $.tajoMonitor.chart.loadSharedLine('#nodeJvmThreadWaitingCount svg', chartNodeData[15]);
        $.tajoMonitor.chart.loadSharedLine('#nodeQueryMasterRunning svg', chartNodeData[16]);
        $.tajoMonitor.chart.loadSharedLine('#nodeTasksRunning svg', chartNodeData[17]);
      });
    };
    // Node
    getAjaxMetaData();
    setInterval(getAjaxMetaData, $.tajoMonitor.TICK_TIME);
    // auto-refresh
    var autoRefresh = $.tajoMonitor.util.getAutoRefresh();
    var refreshInterval = $.tajoMonitor.util.getRefreshInterval();
    if(refreshInterval){
      $('#refreshInterval').val(refreshInterval);
      if(autoRefresh){
        autoRefreshInterval = setInterval(function(){location.reload();}, refreshInterval*1000);
      }
    } else {
      $.tajoMonitor.util.putRefreshInterval(60);  // set default value at 1 min
    }
    if(autoRefresh){
      $('#autoRefresh').attr("checked", autoRefresh);
    }
    $('#refreshInterval').change(function() {
      var newTick = $(this).val();
      $.tajoMonitor.util.putRefreshInterval(newTick);
      if(autoRefreshInterval){
        clearInterval(autoRefreshInterval);
      }
      if($('input:checkbox[id="autoRefresh"]').is(':checked')){
        autoRefreshInterval = setInterval(function(){location.reload();}, newTick*1000);
      }
    });
    $("#autoRefresh").change(function() {
      if($(this).is(":checked")) {
        $.tajoMonitor.util.putAutoRefresh(true);
        var newTick = $.tajoMonitor.util.getRefreshInterval();
        if(autoRefreshInterval){
          clearInterval(autoRefreshInterval);
        }
        autoRefreshInterval = setInterval(function(){location.reload();}, newTick*1000);
      } else {
        $.tajoMonitor.util.putAutoRefresh(false);
        if(autoRefreshInterval){
          clearInterval(autoRefreshInterval);
        }
      }
    });
  } else {
    alert('Oops! Not supported LocalStorage on your browser.');
  }
});
</script>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <!-- Body -->
  <h2>Tajo Node: <%=nodeLabel%></h2>
  <hr/>
  <div>
    <h3>Node</h3>
    <div class="group">
      <div class="group_summary">
        <div class="positionRight">
          Auto Refresh :
          <select id="refreshInterval">
            <option value="60">1 min</option>
            <option value="180">3 min</option>
            <option value="300">5 min</option>
            <option value="600">10 min</option>
          </select>
          <input type="checkbox" id="autoRefresh" />
        </div>
        <div class="positionBottom"/>
      </div>
    </div>
  </div>
  <div>
    <div class="group">
      <div class="chart" id="nodeJvmFile"><svg></svg></div>
      <div class="chart" id="nodeQueryMasterRunning"><svg></svg></div>
      <div class="chart" id="nodeTasksRunning"><svg></svg></div>
    </div>
    <div class="group">
      <div class="chart" id="nodeJvmGcCount"><svg></svg></div>
      <div class="chart" id="nodeJvmGcTime"><svg></svg></div>
      <div class="chart" id="nodeJvmLog"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryHeap"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryHeapUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryNonHeap"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryNonHeapUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryPoolsCodeCacheUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryPoolsPSEdenSpaceUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryPoolsPSOldGenUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryPoolsPSPermGenUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryPoolsPSSurvivorSpaceUsage"><svg></svg></div>
      <div class="chart" id="nodeJvmMemoryTotal"><svg></svg></div>
      <div class="chart" id="nodeJvmThread"><svg></svg></div>
      <div class="chart" id="nodeJvmThreadWaitingCount"><svg></svg></div>
    </div>
  </div>
  <hr />
</div>
</body>
</html>