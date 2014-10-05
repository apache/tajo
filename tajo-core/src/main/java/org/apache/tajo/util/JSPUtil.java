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
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.QueryUnit;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.worker.TaskRunnerHistory;
import org.apache.tajo.worker.TaskRunner;

import java.text.DecimalFormat;
import java.util.*;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class JSPUtil {
  static DecimalFormat decimalF = new DecimalFormat("###.0");

  public static void sortQueryUnit(QueryUnit[] queryUnits, String sortField, String sortOrder) {
    if(sortField == null || sortField.isEmpty()) {
      sortField = "id";
    }

    Arrays.sort(queryUnits, new QueryUnitComparator(sortField, "asc".equals(sortOrder)));
  }

  public static void sortTaskRunner(List<TaskRunner> taskRunners) {
    Collections.sort(taskRunners, new Comparator<TaskRunner>() {
      @Override
      public int compare(TaskRunner taskRunner, TaskRunner taskRunner2) {
        return taskRunner.getId().compareTo(taskRunner2.getId());
      }
    });
  }

  public static void sortTaskRunnerHistory(List<TaskRunnerHistory> histories) {
    Collections.sort(histories, new Comparator<TaskRunnerHistory>() {
      @Override
      public int compare(TaskRunnerHistory h1, TaskRunnerHistory h2) {
        int value = h1.getExecutionBlockId().compareTo(h2.getExecutionBlockId());
        if(value == 0){
          return h1.getContainerId().compareTo(h2.getContainerId());
        }
        return value;
      }
    });
  }

  public static String getElapsedTime(long startTime, long finishTime) {
    if(startTime == 0) {
      return "-";
    }
    return finishTime == 0 ? decimalF.format((System.currentTimeMillis() - startTime) / 1000) + " sec"
        : decimalF.format((finishTime - startTime) / 1000) + " sec";
  }

  public static String getTajoMasterHttpAddr(Configuration config) {
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

  public static List<SubQuery> sortSubQuery(Collection<SubQuery> subQueries) {
    List<SubQuery> subQueryList = new ArrayList<SubQuery>(subQueries);
    Collections.sort(subQueryList, new Comparator<SubQuery>() {
      @Override
      public int compare(SubQuery subQuery1, SubQuery subQuery2) {
        long q1StartTime = subQuery1.getStartTime();
        long q2StartTime = subQuery2.getStartTime();

        q1StartTime = (q1StartTime == 0 ? Long.MAX_VALUE : q1StartTime);
        q2StartTime = (q2StartTime == 0 ? Long.MAX_VALUE : q2StartTime);

        int result = compareLong(q1StartTime, q2StartTime);
        if (result == 0) {
          return subQuery1.getId().toString().compareTo(subQuery2.getId().toString());
        } else {
          return result;
        }
      }
    });

    return subQueryList;
  }

  static class QueryUnitComparator implements Comparator<QueryUnit> {
    private String sortField;
    private boolean asc;
    public QueryUnitComparator(String sortField, boolean asc) {
      this.sortField = sortField;
      this.asc = asc;
    }

    @Override
    public int compare(QueryUnit queryUnit, QueryUnit queryUnit2) {
      if(asc) {
        if("id".equals(sortField)) {
          return queryUnit.getId().compareTo(queryUnit2.getId());
        } else if("host".equals(sortField)) {
          String host1 = queryUnit.getSucceededHost() == null ? "-" : queryUnit.getSucceededHost();
          String host2 = queryUnit2.getSucceededHost() == null ? "-" : queryUnit2.getSucceededHost();
          return host1.compareTo(host2);
        } else if("runTime".equals(sortField)) {
          return compareLong(queryUnit.getRunningTime(), queryUnit2.getRunningTime());
        } else if("startTime".equals(sortField)) {
          return compareLong(queryUnit.getLaunchTime(), queryUnit2.getLaunchTime());
        } else {
          return queryUnit.getId().compareTo(queryUnit2.getId());
        }
      } else {
        if("id".equals(sortField)) {
          return queryUnit2.getId().compareTo(queryUnit.getId());
        } else if("host".equals(sortField)) {
          String host1 = queryUnit.getSucceededHost() == null ? "-" : queryUnit.getSucceededHost();
          String host2 = queryUnit2.getSucceededHost() == null ? "-" : queryUnit2.getSucceededHost();
          return host2.compareTo(host1);
        } else if("runTime".equals(sortField)) {
          if(queryUnit2.getLaunchTime() == 0) {
            return -1;
          } else if(queryUnit.getLaunchTime() == 0) {
            return 1;
          }
          return compareLong(queryUnit2.getRunningTime(), queryUnit.getRunningTime());
        } else if("startTime".equals(sortField)) {
          return compareLong(queryUnit2.getLaunchTime(), queryUnit.getLaunchTime());
        } else {
          return queryUnit2.getId().compareTo(queryUnit.getId());
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

  public static void sortFunctionDesc(List<FunctionDesc> functions) {
    Collections.sort(functions, new java.util.Comparator<FunctionDesc>() {
      @Override
      public int compare(FunctionDesc f1, FunctionDesc f2) {
        int nameCompared = f1.getFunctionName().compareTo(f2.getFunctionName());
        if(nameCompared != 0) {
          return nameCompared;
        } else {
          return f1.getReturnType().getType().compareTo(f2.getReturnType().getType());
        }
      }
    });
  }

  static final DecimalFormat PERCENT_FORMAT = new DecimalFormat("###.#");
  public static String percentFormat(float value) {
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
}
