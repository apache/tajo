/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.QueryInProgress;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.querymaster.QueryMasterTask;
import org.apache.tajo.querymaster.Stage;
import org.apache.tajo.querymaster.Task;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.history.StageHistory;
import org.apache.tajo.util.history.TaskHistory;

import java.text.DecimalFormat;
import java.util.*;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class JSPUtil {
  static DecimalFormat decimalF = new DecimalFormat("###.0");

  public static void sortTaskArray(Task[] tasks, String sortField, String sortOrder) {
    if(sortField == null || sortField.isEmpty()) {
      sortField = "id";
    }

    Arrays.sort(tasks, new TaskComparator(sortField, "asc".equals(sortOrder)));
  }

  public static void sortTasks(List<Task> tasks, String sortField, String sortOrder) {
    if(sortField == null || sortField.isEmpty()) {
      sortField = "id";
    }

    Collections.sort(tasks, new TaskComparator(sortField, "asc".equals(sortOrder)));
  }

  public static void sortTaskHistory(List<TaskHistory> tasks, String sortField, String sortOrder) {
    if(sortField == null || sortField.isEmpty()) {
      sortField = "id";
    }

    Collections.sort(tasks, new TaskHistoryComparator(sortField, "asc".equals(sortOrder)));
  }

  public static String getElapsedTime(long startTime, long finishTime) {
    if(startTime == 0) {
      return "-";
    }
    return finishTime == 0 ? decimalF.format((System.currentTimeMillis() - startTime) / 1000) + " sec"
        : decimalF.format((finishTime - startTime) / 1000) + " sec";
  }

  public static String getTajoMasterHttpAddr(Configuration config) {
    if (!(config instanceof TajoConf)) {
      throw new IllegalArgumentException("config should be a TajoConf type.");
    }
    try {
      TajoConf conf = (TajoConf) config;
      String [] masterAddr = conf.getVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS).split(":");
      return masterAddr[0] + ":" + conf.getVar(ConfVars.TAJO_MASTER_INFO_ADDRESS).split(":")[1];
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  public static List<QueryMasterTask> sortQueryMasterTask(Collection<QueryMasterTask> queryMasterTasks,
                                                          final boolean desc) {
    List<QueryMasterTask> queryMasterTaskList = new ArrayList<QueryMasterTask>(queryMasterTasks);

    Collections.sort(queryMasterTaskList, new Comparator<QueryMasterTask>() {

      @Override
      public int compare(QueryMasterTask task1, QueryMasterTask task2) {
        if(desc) {
          return task2.getQueryId().toString().compareTo(task1.getQueryId().toString());
        } else {
          return task1.getQueryId().toString().compareTo(task2.getQueryId().toString());
        }
      }
    });

    return queryMasterTaskList;
  }

  public static List<QueryInProgress> sortQueryInProgress(Collection<QueryInProgress> queryInProgresses,
                                                          final boolean desc) {
    List<QueryInProgress> queryProgressList = new ArrayList<QueryInProgress>(queryInProgresses);

    Collections.sort(queryProgressList, new Comparator<QueryInProgress>() {
      @Override
      public int compare(QueryInProgress query1, QueryInProgress query2) {
        if(desc) {
          return query2.getQueryId().toString().compareTo(query1.getQueryId().toString());
        } else {
          return query1.getQueryId().toString().compareTo(query2.getQueryId().toString());
        }
      }
    });

    return queryProgressList;
  }

  public static List<Stage> sortStages(Collection<Stage> stages) {
    List<Stage> stageList = new ArrayList<Stage>(stages);
    Collections.sort(stageList, new Comparator<Stage>() {
      @Override
      public int compare(Stage stage1, Stage stage2) {
        long q1StartTime = stage1.getStartTime();
        long q2StartTime = stage2.getStartTime();

        q1StartTime = (q1StartTime == 0 ? Long.MAX_VALUE : q1StartTime);
        q2StartTime = (q2StartTime == 0 ? Long.MAX_VALUE : q2StartTime);

        int result = compareLong(q1StartTime, q2StartTime);
        if (result == 0) {
          return stage1.getId().toString().compareTo(stage2.getId().toString());
        } else {
          return result;
        }
      }
    });

    return stageList;
  }

  public static List<StageHistory> sortStageHistories(Collection<StageHistory> stages) {
    List<StageHistory> stageList = new ArrayList<StageHistory>(stages);
    Collections.sort(stageList, new Comparator<StageHistory>() {
      @Override
      public int compare(StageHistory stage1, StageHistory stage2) {
        long q1StartTime = stage1.getStartTime();
        long q2StartTime = stage2.getStartTime();

        q1StartTime = (q1StartTime == 0 ? Long.MAX_VALUE : q1StartTime);
        q2StartTime = (q2StartTime == 0 ? Long.MAX_VALUE : q2StartTime);

        int result = compareLong(q1StartTime, q2StartTime);
        if (result == 0) {
          return stage1.getExecutionBlockId().compareTo(stage2.getExecutionBlockId());
        } else {
          return result;
        }
      }
    });

    return stageList;
  }

  public static String getMasterActiveLabel(MasterContext context) {
    ServiceTracker haService = context.getHAService();
    String activeLabel = "";
    if (haService != null) {
      if (haService.isActiveMaster()) {
        activeLabel = "<font color='#1e90ff'>(active)</font>";
      } else {
        activeLabel = "<font color='#1e90ff'>(backup)</font>";
      }
    }

    return activeLabel;
  }

  static class TaskComparator implements Comparator<Task> {
    private String sortField;
    private boolean asc;
    public TaskComparator(String sortField, boolean asc) {
      this.sortField = sortField;
      this.asc = asc;
    }

    @Override
    public int compare(Task task, Task task2) {
      if(asc) {
        if("id".equals(sortField)) {
          return task.getId().compareTo(task2.getId());
        } else if("host".equals(sortField)) {
          String host1 = task.getSucceededWorker() == null ? "-" : task.getSucceededWorker().getHost();
          String host2 = task2.getSucceededWorker() == null ? "-" : task2.getSucceededWorker().getHost();
          return host1.compareTo(host2);
        } else if("runTime".equals(sortField)) {
          return compareLong(task.getRunningTime(), task2.getRunningTime());
        } else if("startTime".equals(sortField)) {
          return compareLong(task.getLaunchTime(), task2.getLaunchTime());
        } else {
          return task.getId().compareTo(task2.getId());
        }
      } else {
        if("id".equals(sortField)) {
          return task2.getId().compareTo(task.getId());
        } else if("host".equals(sortField)) {
          String host1 = task.getSucceededWorker() == null ? "-" : task.getSucceededWorker().getHost();
          String host2 = task2.getSucceededWorker() == null ? "-" : task2.getSucceededWorker().getHost();
          return host2.compareTo(host1);
        } else if("runTime".equals(sortField)) {
          if(task2.getLaunchTime() == 0) {
            return -1;
          } else if(task.getLaunchTime() == 0) {
            return 1;
          }
          return compareLong(task2.getRunningTime(), task.getRunningTime());
        } else if("startTime".equals(sortField)) {
          return compareLong(task2.getLaunchTime(), task.getLaunchTime());
        } else {
          return task2.getId().compareTo(task.getId());
        }
      }
    }
  }

  static class TaskHistoryComparator implements Comparator<TaskHistory> {
    private String sortField;
    private boolean asc;
    public TaskHistoryComparator(String sortField, boolean asc) {
      this.sortField = sortField;
      this.asc = asc;
    }

    @Override
    public int compare(TaskHistory task1, TaskHistory task2) {
      if(asc) {
        if("id".equals(sortField)) {
          return task1.getId().compareTo(task2.getId());
        } else if("host".equals(sortField)) {
          String host1 = task1.getHostAndPort() == null ? "-" : task1.getHostAndPort();
          String host2 = task2.getHostAndPort() == null ? "-" : task2.getHostAndPort();
          return host1.compareTo(host2);
        } else if("runTime".equals(sortField)) {
          return compareLong(task1.getRunningTime(), task2.getRunningTime());
        } else if("startTime".equals(sortField)) {
          return compareLong(task1.getLaunchTime(), task2.getLaunchTime());
        } else {
          return task1.getId().compareTo(task2.getId());
        }
      } else {
        if("id".equals(sortField)) {
          return task2.getId().compareTo(task1.getId());
        } else if("host".equals(sortField)) {
          String host1 = task1.getHostAndPort() == null ? "-" : task1.getHostAndPort();
          String host2 = task2.getHostAndPort() == null ? "-" : task2.getHostAndPort();
          return host2.compareTo(host1);
        } else if("runTime".equals(sortField)) {
          if(task2.getLaunchTime() == 0) {
            return -1;
          } else if(task1.getLaunchTime() == 0) {
            return 1;
          }
          return compareLong(task2.getRunningTime(), task1.getRunningTime());
        } else if("startTime".equals(sortField)) {
          return compareLong(task2.getLaunchTime(), task1.getLaunchTime());
        } else {
          return task2.getId().compareTo(task1.getId());
        }
      }
    }
  }

  static int compareLong(long a, long b) {
    if(a > b) {
      return 1;
    } else if(a < b) {
      return -1;
    } else {
      return 0;
    }
  }

  static final DecimalFormat PERCENT_FORMAT = new DecimalFormat("###.#");

  public static String percentFormat(float value) {
    if (Float.isInfinite(value) || Float.isNaN(value)) {
      value = 0.0f;
    }
    return PERCENT_FORMAT.format(value * 100.0f);
  }

  public static String tableStatToString(TableStats tableStats) {
    if(tableStats != null){
      return tableStatToString(tableStats.getProto());
    }
    else {
      return "No input statistics";
    }
  }

  public static String tableStatToString(CatalogProtos.TableStatsProto tableStats) {
    if (tableStats == null) {
      return "No input statistics";
    }

    String result = "";
    result += "TotalBytes: " + FileUtil.humanReadableByteCount(tableStats.getNumBytes(), false) + " ("
        + tableStats.getNumBytes() + " B)";
    result += ", ReadBytes: " + FileUtil.humanReadableByteCount(tableStats.getReadBytes(), false) + " ("
        + tableStats.getReadBytes() + " B)";
    result += ", ReadRows: " + (tableStats.getNumRows() == 0 ? "-" : tableStats.getNumRows());

    return result;
  }

  public static String getPageNavigation(int currentPage, int totalPage, String url) {
    StringBuilder sb = new StringBuilder();

    int pageIndex = (currentPage - 1) / 10;
    int totalPageIndex = (totalPage - 1) / 10;

    String prefix = "";

    if (pageIndex > 0) {
      int prevPage = pageIndex * 10;
      sb.append(prefix).append("<a href='").append(url)
          .append("&page=").append(prevPage).append("'>")
          .append("&lt;</a>");
      prefix = "&nbsp;&nbsp;";
    }

    for (int i = 1; i <= 10; i++) {
      int printPage = pageIndex * 10 + i;
      if (printPage == currentPage) {
        sb.append(prefix).append(printPage);
      } else {
        sb.append(prefix).append("<a href='").append(url)
            .append("&page=").append(printPage).append("'>")
            .append("[").append(printPage).append("]</a>");
      }
      prefix = "&nbsp;&nbsp;";
      if (printPage >= totalPage) {
        break;
      }
    }

    if(totalPageIndex > pageIndex) {
      int nextPage = (pageIndex + 1) * 10 + 1;
      sb.append(prefix).append("<a href='").append(url)
          .append("&page=").append(nextPage).append("'>")
          .append("&gt;</a>");
    }
    return sb.toString();
  }

  public static String getPageNavigation(int currentPage, boolean next, String url) {
    StringBuilder sb = new StringBuilder();
    if (currentPage > 1) {
      sb.append("<a href='").append(url)
          .append("&page=").append(currentPage - 1).append("'>")
          .append("&lt;prev</a>");
      sb.append("&nbsp;&nbsp;");
    }

    sb.append(currentPage);

    if(next) {
      sb.append("&nbsp;&nbsp;").append("<a href='").append(url)
          .append("&page=").append(currentPage + 1).append("'>")
          .append("next&gt;</a>");
    }
    return sb.toString();
  }

  public static <T extends Object> List<T> getPageNavigationList(List<T> originList, int page, int pageSize) {
    if (originList == null) {
      return new ArrayList<T>();
    }
    int start = (page - 1) * pageSize;
    int end = start + pageSize;
    if (end > originList.size()) {
      end = originList.size();
    }
    if (!originList.isEmpty()) {
      return originList.subList(start, end);
    }

    return originList;
  }
}
