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

package org.apache.tajo.plan.logical;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.util.TUtil;

public class IndexScanNode extends ScanNode {
  @Expose private SortSpec [] keySortSpecs;
  @Expose private Schema keySchema = null;
  @Expose private Datum[] predicateValues = null;
  @Expose private Path indexPath = null;
  
  public IndexScanNode(int pid, ScanNode scanNode ,
      Schema keySchema , SimplePredicate[] predicates, Path indexPath) {
    super(pid);
    init(scanNode.getTableDesc());
    setQual(scanNode.getQual());
    setInSchema(scanNode.getInSchema());
    setTargets(scanNode.getTargets());
    setType(NodeType.INDEX_SCAN);
    this.keySchema = keySchema;
    this.indexPath = indexPath;
    keySortSpecs = new SortSpec[predicates.length];
    predicateValues = new Datum[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      keySortSpecs[i] = predicates[i].getSortSpec();
      predicateValues[i] = predicates[i].getValue();
    }
  }
  
  public SortSpec[] getKeySortSpecs() {
    return this.keySortSpecs;
  }
  
  public Schema getKeySchema() {
    return this.keySchema;
  }
  
  public Datum[] getPredicateValues() {
    return this.predicateValues;
  }
  
  public void setKeySortSpecs(SortSpec[] keySortSpecs) {
    this.keySortSpecs = keySortSpecs;
  }
  
  public void setKeySchema( Schema keySchema ) {
    this.keySchema = keySchema;
  }

  @Override
  public String toString() {
    Gson gson = PlanGsonHelper.getInstance();
    StringBuilder builder = new StringBuilder();
    builder.append("IndexScanNode : {\n");
    builder.append("  \"indexPath\" : \"" + gson.toJson(this.indexPath) + "\"\n");
    builder.append("  \"keySchema\" : \"" + gson.toJson(this.keySchema) + "\"\n");
    builder.append("  \"keySortSpecs\" : \"" + gson.toJson(this.keySortSpecs) + " \"\n");
    builder.append("  \"datums\" : \"" + gson.toJson(this.predicateValues) + "\"\n");
    builder.append("      <<\"superClass\" : " + super.toString());
    builder.append(">>}");
    builder.append("}");
    return builder.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IndexScanNode) {
      IndexScanNode other = (IndexScanNode) obj;
      return super.equals(other) &&
          this.indexPath.equals(other.indexPath) &&
          TUtil.checkEquals(this.keySortSpecs, other.keySortSpecs) &&
          TUtil.checkEquals(this.predicateValues, other.predicateValues) &&
          this.keySchema.equals(other.keySchema);
    }   
    return false;
  } 
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    IndexScanNode indexNode = (IndexScanNode) super.clone();
    indexNode.keySchema = (Schema) this.keySchema.clone();
    indexNode.keySortSpecs = new SortSpec[this.keySortSpecs.length];
    for(int i = 0 ; i < keySortSpecs.length ; i ++ )
      indexNode.keySortSpecs[i] = (SortSpec) this.keySortSpecs[i].clone();
    indexNode.predicateValues = new Datum[this.predicateValues.length];
    for(int i = 0 ; i < predicateValues.length ; i ++ ) {
      indexNode.predicateValues[i] = this.predicateValues[i];
    }
    indexNode.indexPath = this.indexPath;
    return indexNode;
  }

  public Path getIndexPath() {
    return indexPath;
  }

  public void setIndexPath(Path indexPath) {
    this.indexPath = indexPath;
  }
}


