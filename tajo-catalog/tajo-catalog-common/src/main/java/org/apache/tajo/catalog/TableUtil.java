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

package org.apache.tajo.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.util.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TableUtil {
  public static TableMeta getTableMeta(Configuration conf, Path tablePath) 
      throws IOException {
    TableMeta meta = null;
    
    FileSystem fs = tablePath.getFileSystem(conf);
    
    Path tableMetaPath = new Path(tablePath, ".meta");
    if(!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in "+tablePath.toString());
    }
    FSDataInputStream tableMetaIn = 
      fs.open(tableMetaPath);

    TableProto tableProto = (TableProto) FileUtil.loadProto(tableMetaIn, 
      TableProto.getDefaultInstance());
    meta = new TableMeta(tableProto);

    return meta;
  }
}
