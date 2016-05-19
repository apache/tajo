/*
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.PartitionedTableScanNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.partition.PartitionPruningHandle;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.querymaster.Stage;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SplitUtil {

  /**
   * This method creates fragments depending on the table type. If the table is
   * a partitioned table, it will creates multiple fragments for all partitions.
   * Otherwise, it creates at least one fragments for a table, which may
   * span a number of blocks or possibly consists of a number of files.
   *
   * Also, we can ensure FileTableSpace if the type of table is a partitioned table.
   *
   * @param tablespace tablespace handler
   * @param scan scan node
   * @param tableDesc table desc of scan node
   * @param requireSort if set, the result fragments will be sorted with their paths.
   *                    Only set when a query type is the simple query.
   * @return a list of fragments for input table
   * @throws IOException
   * @throws TajoException
   */
  public static List<Fragment> getSplits(Tablespace tablespace,
                                         ScanNode scan,
                                         TableDesc tableDesc,
                                         boolean requireSort,
                                         CatalogService catalog,
                                         TajoConf conf)
      throws IOException, TajoException {
    List<Fragment> fragments;
    if (tableDesc.hasPartition()) {
      fragments = SplitUtil.getFragmentsFromPartitionedTable(tablespace, scan, tableDesc, requireSort, catalog, conf);
    } else {
      fragments = tablespace.getSplits(scan.getCanonicalName(), tableDesc, requireSort, scan.getQual());
    }

    return fragments;
  }

  /**
   * It creates a number of fragments for all partitions.
   */
  private static List<Fragment> getFragmentsFromPartitionedTable(Tablespace tsHandler,
                                                                 ScanNode scan,
                                                                 TableDesc table,
                                                                 boolean requireSort,
                                                                 CatalogService catalog,
                                                                 TajoConf conf) throws IOException, TajoException {
    Preconditions.checkArgument(tsHandler instanceof FileTablespace, "tsHandler must be FileTablespace");
    if (!(scan instanceof PartitionedTableScanNode)) {
      throw new IllegalArgumentException("scan should be a PartitionedTableScanNode type.");
    }
    List<Fragment> fragments = Lists.newArrayList();
    PartitionedTableScanNode partitionsScan = (PartitionedTableScanNode) scan;
    partitionsScan.init(scan);
    PartitionedTableRewriter rewriter = new PartitionedTableRewriter();
    rewriter.setCatalog(catalog);
    PartitionPruningHandle pruningHandle = rewriter.getPartitionPruningHandle(conf, partitionsScan);

    FileTablespace tablespace = (FileTablespace) tsHandler;
    fragments.addAll(tablespace.getPartitionSplits(scan.getCanonicalName(), table.getMeta(), table.getSchema()
      , requireSort, pruningHandle.getPartitionKeys(), pruningHandle.getPartitionPaths()));

    return fragments;

  }
}
