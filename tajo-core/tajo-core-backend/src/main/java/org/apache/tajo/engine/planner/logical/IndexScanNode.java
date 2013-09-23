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

package org.apache.tajo.engine.planner.logical;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.json.CoreGsonHelper;

public class IndexScanNode extends ScanNode {
  @Expose private SortSpec [] sortKeys;
  @Expose private Schema keySchema = null;
  @Expose private Datum[] datum = null;
  
  public IndexScanNode(int pid, ScanNode scanNode ,
      Schema keySchema , Datum[] datum, SortSpec[] sortKeys ) {
    super(pid, scanNode.getTableDesc());
    setQual(scanNode.getQual());
    setInSchema(scanNode.getInSchema());
    setOutSchema(scanNode.getOutSchema());
    setTargets(scanNode.getTargets());
    setType(NodeType.BST_INDEX_SCAN);
    this.sortKeys = sortKeys;
    this.keySchema = keySchema;
    this.datum = datum;
  }
  
  public SortSpec[] getSortKeys() {
    return this.sortKeys;
  }
  
  public Schema getKeySchema() {
    return this.keySchema;
  }
  
  public Datum[] getDatum() {
    return this.datum;
  }
  
  public void setSortKeys(SortSpec[] sortKeys) {
    this.sortKeys = sortKeys;
  }
  
  public void setKeySchema( Schema keySchema ) {
    this.keySchema = keySchema;
  }

  @Override
  public String toString() {
    Gson gson = CoreGsonHelper.getInstance();
    StringBuilder builder = new StringBuilder();
    builder.append("IndexScanNode : {\n");
    builder.append("  \"keySchema\" : \"" + gson.toJson(this.keySchema) + "\"\n");
    builder.append("  \"sortKeys\" : \"" + gson.toJson(this.sortKeys) + " \"\n");
    builder.append("  \"datums\" : \"" + gson.toJson(this.datum) + "\"\n");
    builder.append("      <<\"superClass\" : " + super.toString());
    builder.append(">>}");
    builder.append("}");
    return builder.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IndexScanNode) {
      IndexScanNode other = (IndexScanNode) obj;
      
      boolean eq = super.equals(other);
      eq = eq && this.sortKeys.length == other.sortKeys.length;
      if(eq) {
        for(int i = 0 ; i < this.sortKeys.length ; i ++) {
          eq = eq && this.sortKeys[i].getSortKey().equals(
              other.sortKeys[i].getSortKey());
          eq = eq && this.sortKeys[i].isAscending()
              == other.sortKeys[i].isAscending();
          eq = eq && this.sortKeys[i].isNullFirst()
              == other.sortKeys[i].isNullFirst();
        }
      }
      if(eq) {
        for(int i = 0 ; i < this.datum.length ; i ++ ) {
          eq = eq && this.datum[i].equals(other.datum[i]);
        }
      }
     return eq;
    }   
    return false;
  } 
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    IndexScanNode indexNode = (IndexScanNode) super.clone();
    indexNode.keySchema = (Schema) this.keySchema.clone();
    indexNode.sortKeys = new SortSpec[this.sortKeys.length];
    for(int i = 0 ; i < sortKeys.length ; i ++ )
      indexNode.sortKeys[i] = (SortSpec) this.sortKeys[i].clone();
    indexNode.datum = new Datum[this.datum.length];
    for(int i = 0 ; i < datum.length ; i ++ ) {
      indexNode.datum[i] = this.datum[i];
    }
    return indexNode;
  }
}


