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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.rewrite.rules.IndexScanInfo.SimplePredicate;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.util.TUtil;

import java.net.URI;

public class IndexScanNode extends ScanNode {
  @Expose private Schema keySchema = null;
  @Expose private URI indexPath = null;
  @Expose private SimplePredicate[] predicates = null;

  public IndexScanNode(int pid) {
    super(pid);
    setType(NodeType.INDEX_SCAN);
  }
  
  public IndexScanNode(int pid, ScanNode scanNode ,
      Schema keySchema , SimplePredicate[] predicates, URI indexPath) {
    this(pid);
    init(scanNode.getTableDesc());
    setQual(scanNode.getQual());
    setInSchema(scanNode.getInSchema());
    setTargets(scanNode.getTargets());
    this.set(keySchema, predicates, indexPath);
  }

  public void set(Schema keySchema, SimplePredicate[] predicates, URI indexPath) {
    this.keySchema = keySchema;
    this.indexPath = indexPath;
    this.predicates = predicates;
  }
  
  public Schema getKeySchema() {
    return this.keySchema;
  }

  public SimplePredicate[] getPredicates() {
    return predicates;
  }
  
  @Override
  public String toString() {
    Gson gson = PlanGsonHelper.getInstance();
    StringBuilder builder = new StringBuilder();
    builder.append("IndexScanNode : {\n");
    builder.append("  \"indexPath\" : \"" + gson.toJson(this.indexPath) + "\"\n");
    builder.append("  \"keySchema\" : \"" + gson.toJson(this.keySchema) + "\"\n");
    builder.append("  \"keySortSpecs\" : \"" + gson.toJson(predicates) + " \"\n");
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
      eq &= this.indexPath.equals(other.indexPath);
      eq &= TUtil.checkEquals(this.predicates, other.predicates);
      eq &= this.keySchema.equals(other.keySchema);

      return eq;
    }   
    return false;
  } 
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    IndexScanNode indexNode = (IndexScanNode) super.clone();
    indexNode.keySchema = (Schema) this.keySchema.clone();
    indexNode.predicates = new SimplePredicate[this.predicates.length];
    for(int i = 0 ; i < this.predicates.length ; i ++ )
      indexNode.predicates[i] = (SimplePredicate) this.predicates[i].clone();
    indexNode.indexPath = this.indexPath;
    return indexNode;
  }

  public URI getIndexPath() {
    return indexPath;
  }
}


