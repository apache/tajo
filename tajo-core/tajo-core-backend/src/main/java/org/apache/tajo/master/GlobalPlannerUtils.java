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

package org.apache.tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.planner.FromTable;
import org.apache.tajo.engine.planner.logical.ScanNode;

public class GlobalPlannerUtils {
  private static Log LOG = LogFactory.getLog(GlobalPlannerUtils.class);

  public static ScanNode newScanPlan(Schema inputSchema,
                                     String inputTableId,
                                     Path inputPath) {
    TableMeta meta = CatalogUtil.newTableMeta(inputSchema, StoreType.CSV);
    TableDesc desc = CatalogUtil.newTableDesc(inputTableId, meta, inputPath);
    ScanNode newScan = new ScanNode(new FromTable(desc));
    newScan.setInSchema(desc.getMeta().getSchema());
    newScan.setOutSchema(desc.getMeta().getSchema());
    return newScan;
  }
}
