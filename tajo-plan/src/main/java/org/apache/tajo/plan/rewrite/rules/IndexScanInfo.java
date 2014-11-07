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

package org.apache.tajo.plan.rewrite.rules;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil.ColumnNameComparatorOfSortSpec;
import org.apache.tajo.catalog.IndexDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.TUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class IndexScanInfo extends AccessPathInfo {

  public static class SimplePredicate {
    private SortSpec sortSpec;
    private Datum value;

    public SimplePredicate(SortSpec sortSpec, Datum value) {
      this.sortSpec = sortSpec;
      this.value = value;
    }

    public SortSpec getSortSpec() {
      return sortSpec;
    }

    public Datum getValue() {
      return value;
    }
  }

  private Path indexPath;
  private Schema keySchema;
  private SimplePredicate[] predicates;

  public IndexScanInfo(TableStats tableStats) {
    super(ScanTypeControl.INDEX_SCAN, tableStats);
  }

  public IndexScanInfo(TableStats tableStats, IndexDesc indexDesc, Datum[] values) {
    this(tableStats);
    this.indexPath = indexDesc.getIndexPath();
    initPredicates(indexDesc, values);
  }

  private void initPredicates(IndexDesc desc, Datum[] values) {
    this.predicates = new SimplePredicate[values.length];
    Map<TajoDataTypes.Type, Datum> valueMap = TUtil.newHashMap();
    for (Datum val : values) {
      valueMap.put(val.type(), val);
    }
    List<SortSpec> modifiableKeySortSpecs = TUtil.newList(desc.getKeySortSpecs());
    Collections.sort(modifiableKeySortSpecs, new ColumnNameComparatorOfSortSpec());

    keySchema = new Schema();
    int i = 0;
    for (SortSpec keySortSpec : modifiableKeySortSpecs) {
      predicates[i++] = new SimplePredicate(keySortSpec,
          valueMap.get(keySortSpec.getSortKey().getDataType().getType()));
      keySchema.addColumn(keySortSpec.getSortKey());
    }
  }

  public Path getIndexPath() {
    return indexPath;
  }

  public Schema getKeySchema() {
    return keySchema;
  }

  public SimplePredicate[] getPredicates() {
    return predicates;
  }
}
