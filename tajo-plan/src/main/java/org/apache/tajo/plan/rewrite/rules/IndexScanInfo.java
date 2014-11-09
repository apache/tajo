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

import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.IndexDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;

public class IndexScanInfo extends AccessPathInfo {

  /**
   * Simple predicate represents an equal eval expression which consists of
   * a column and a value.
   */
  // TODO: extend to represent more complex expressions
  public static class SimplePredicate {
    @Expose private SortSpec sortSpec;
    @Expose private Datum value;

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

    @Override
    public boolean equals(Object o) {
      if (o instanceof SimplePredicate) {
        SimplePredicate other = (SimplePredicate) o;
        return this.sortSpec.equals(other.sortSpec) && this.value.equals(other.sortSpec);
      } else {
        return false;
      }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      SimplePredicate clone = new SimplePredicate(this.sortSpec, this.value);
      return clone;
    }
  }

  private final Path indexPath;
  private final Schema keySchema;
  private final SimplePredicate[] predicates;

//  public IndexScanInfo(TableStats tableStats) {
//    super(ScanTypeControl.INDEX_SCAN, tableStats);
//  }

  public IndexScanInfo(TableStats tableStats, IndexDesc indexDesc, SimplePredicate[] predicates) {
    super(ScanTypeControl.INDEX_SCAN, tableStats);
    this.indexPath = indexDesc.getIndexPath();
    keySchema = new Schema();
    this.predicates = predicates;
    for (SimplePredicate predicate : predicates) {
      keySchema.addColumn(predicate.getSortSpec().getSortKey());
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
