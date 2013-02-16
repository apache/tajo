/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

/**
 *
 */
package tajo;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.datum.DatumFactory;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.*;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.engine.query.ResultSetImpl;
import tajo.storage.*;
import tajo.util.FileUtil;
import tajo.util.TUtil;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;

public class BackendTestingUtil {
	public final static Schema mockupSchema;
	public final static TableMeta mockupMeta;

	static {
    mockupSchema = new Schema();
    mockupSchema.addColumn("deptname", DataType.STRING);
    mockupSchema.addColumn("score", DataType.INT);
    mockupMeta = TCatUtil.newTableMeta(mockupSchema, StoreType.CSV);
	}

  public static void writeTmpTable(TajoConf conf, Path path,
                                   String tableName, boolean writeMeta)
      throws IOException {
    StorageManager sm = StorageManager.get(conf, path);
    FileSystem fs = sm.getFileSystem();
    Appender appender;

    Path tablePath = StorageUtil.concatPath(path, tableName, "table.csv");
    if (fs.exists(tablePath.getParent())) {
      fs.delete(tablePath.getParent(), true);
    }
    fs.mkdirs(tablePath.getParent());

    if (writeMeta) {
      FileUtil.writeProto(fs, new Path(tablePath.getParent(), ".meta"), mockupMeta.getProto());
    }
    appender = StorageManager.getAppender(conf, mockupMeta, tablePath);
    appender.init();

    int deptSize = 10000;
    int tupleNum = 100;
    Tuple tuple;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createString(key));
      tuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(tuple);
    }
    appender.close();
  }

	public static void writeTmpTable(TajoConf conf, String parent,
	    String tableName, boolean writeMeta) throws IOException {
    writeTmpTable(conf, new Path(parent), tableName, writeMeta);
	}

  private TajoConf conf;
  private CatalogService catalog;
  private QueryAnalyzer analyzer;
  private LogicalPlanner planner;
  public BackendTestingUtil(TajoConf conf) throws IOException {
    this.conf = conf;
    this.catalog = new LocalCatalog(conf);
    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
  }

  public ResultSet run(String [] tableNames, File [] tables, Schema [] schemas, String query)
      throws IOException {
    Path workDir = createTmpTestDir();
    StorageManager sm = StorageManager.get(new TajoConf(), workDir);
    List<Fragment> frags = Lists.newArrayList();
    for (int i = 0; i < tableNames.length; i++) {
      Fragment [] splits = sm.split(tableNames[i], new Path(tables[i].getAbsolutePath()));
      for (Fragment f : splits) {
        frags.add(f);
      }
    }

    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(),
        frags.toArray(new Fragment[frags.size()]), workDir);
    PlanningContext context = analyzer.parse(query);
    LogicalNode plan = planner.createPlan(context);
    plan = LogicalOptimizer.optimize(context, plan);
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    return new ResultSetImpl(conf, new Path(workDir, "out"));
  }

  public static Path createTmpTestDir() throws IOException {
    String randomStr = UUID.randomUUID().toString();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path dir = new Path("target/test-data", randomStr);
    // Have it cleaned up on exit
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }
    return dir;
  }
}
