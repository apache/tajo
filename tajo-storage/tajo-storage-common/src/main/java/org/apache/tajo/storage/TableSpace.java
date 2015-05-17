/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * It manages each tablespace; e.g., HDFS, Local file system, and Amazon S3.
 */
public interface TableSpace extends Closeable {
  //public void format() throws IOException;

  void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException;

  void purgeTable(TableDesc tableDesc) throws IOException;

  List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException;

  List<Fragment> getSplits(String fragmentId, TableDesc tableDesc) throws IOException;

//  public void renameTable() throws IOException;
//
//  public void truncateTable() throws IOException;
//
//  public long availableCapacity() throws IOException;
//
//  public long totalCapacity() throws IOException;

  Scanner getScanner(TableMeta meta, Schema schema, CatalogProtos.FragmentProto fragment, Schema target) throws IOException;

  Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment) throws IOException;

  Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException;

  SeekableScanner getSeekableScanner(TableMeta meta, Schema schema, CatalogProtos.FragmentProto fragment, Schema target)
      throws IOException;

  Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId,
                               LogicalPlan plan, Schema schema,
                               TableDesc tableDesc) throws IOException;

  void verifyInsertTableSchema(TableDesc tableDesc, Schema outSchema) throws IOException;

  List<LogicalPlanRewriteRule> getRewriteRules(OverridableConf queryContext, TableDesc tableDesc) throws IOException;

  void close() throws IOException;
}
