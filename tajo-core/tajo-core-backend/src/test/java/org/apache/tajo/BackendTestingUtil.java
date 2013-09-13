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

/**
 *
 */
package org.apache.tajo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.FileUtil;

import java.io.IOException;

public class BackendTestingUtil {
	public final static Schema mockupSchema;
	public final static TableMeta mockupMeta;

	static {
    mockupSchema = new Schema();
    mockupSchema.addColumn("deptname", Type.TEXT);
    mockupSchema.addColumn("score", Type.INT4);
    mockupMeta = CatalogUtil.newTableMeta(mockupSchema, StoreType.CSV);
	}

  public static void writeTmpTable(TajoConf conf, Path path,
                                   String tableName, boolean writeMeta)
      throws IOException {
    AbstractStorageManager sm = StorageManagerFactory.getStorageManager(conf, path);
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
    appender = StorageManagerFactory.getStorageManager(conf).getAppender(mockupMeta, tablePath);
    appender.init();

    int deptSize = 10000;
    int tupleNum = 100;
    Tuple tuple;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createText(key));
      tuple.put(1, DatumFactory.createInt4(i + 1));
      appender.addTuple(tuple);
    }
    appender.close();
  }

	public static void writeTmpTable(TajoConf conf, String parent,
	    String tableName, boolean writeMeta) throws IOException {
    writeTmpTable(conf, new Path(parent), tableName, writeMeta);
	}

  public BackendTestingUtil(TajoConf conf) throws IOException {
  }
}
