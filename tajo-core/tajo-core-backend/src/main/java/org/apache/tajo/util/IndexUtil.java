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

package org.apache.tajo.util;

import com.google.gson.Gson;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.logical.IndexScanNode;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.storage.Fragment;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

public class IndexUtil {
  public static String getIndexNameOfFrag(Fragment fragment, SortSpec[] keys) {
    StringBuilder builder = new StringBuilder(); 
    builder.append(fragment.getPath().getName() + "_");
    builder.append(fragment.getStartOffset() + "_" + fragment.getLength() + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getSortKey().getColumnName()+"_");
    }
    builder.append("_index");
    return builder.toString();
       
  }
  
  public static String getIndexName(String indexName , SortSpec[] keys) {
    StringBuilder builder = new StringBuilder();
    builder.append(indexName + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getSortKey().getColumnName() + "_");
    }
    return builder.toString();
  }
  
  public static IndexScanNode indexEval(LogicalPlan plan, ScanNode scanNode,
      Iterator<Entry<String, String>> iter ) {
   
    EvalNode qual = scanNode.getQual();
    Gson gson = CoreGsonHelper.getInstance();
    
    FieldAndValueFinder nodeFinder = new FieldAndValueFinder();
    qual.preOrder(nodeFinder);
    LinkedList<EvalNode> nodeList = nodeFinder.getNodeList();
    
    int maxSize = Integer.MIN_VALUE;
    SortSpec[] maxIndex = null;
    
    String json;
    while(iter.hasNext()) {
      Entry<String , String> entry = iter.next();
      json = entry.getValue();
      SortSpec[] sortKey = gson.fromJson(json, SortSpec[].class);
      if(sortKey.length > nodeList.size()) {
        /* If the number of the sort key is greater than where condition, 
         * this index cannot be used
         * */
        continue; 
      } else {
        boolean[] equal = new boolean[sortKey.length];
        for(int i = 0 ; i < sortKey.length ; i ++) {
          for(int j = 0 ; j < nodeList.size() ; j ++) {
            Column col = ((FieldEval)(nodeList.get(j).getLeftExpr())).getColumnRef();
            if(col.equals(sortKey[i].getSortKey())) {
              equal[i] = true;
            }
          }
        }
        boolean chk = true;
        for(int i = 0 ; i < equal.length ; i ++) {
          chk = chk && equal[i];
        }
        if(chk) {
          if(maxSize < sortKey.length) {
            maxSize = sortKey.length;
            maxIndex = sortKey;
          }
        }
      }
    }
    if(maxIndex == null) {
      return null;
    } else {
      Schema keySchema = new Schema();
      for(int i = 0 ; i < maxIndex.length ; i ++ ) {
        keySchema.addColumn(maxIndex[i].getSortKey());
      }
      Datum[] datum = new Datum[nodeList.size()];
      for(int i = 0 ; i < nodeList.size() ; i ++ ) {
        datum[i] = ((ConstEval)(nodeList.get(i).getRightExpr())).getValue();
      }
      
      return new IndexScanNode(plan.newPID(), scanNode, keySchema , datum , maxIndex);
    }

  }
  
  
  private static class FieldAndValueFinder implements EvalNodeVisitor {
    private LinkedList<EvalNode> nodeList = new LinkedList<EvalNode>();
    
    public LinkedList<EvalNode> getNodeList () {
      return this.nodeList;
    }
    
    @Override
    public void visit(EvalNode node) {
      switch(node.getType()) {
      case AND:
        break;
      case EQUAL:
        if( node.getLeftExpr().getType() == EvalType.FIELD
          && node.getRightExpr().getType() == EvalType.CONST ) {
          nodeList.add(node);
        }
        break;
      case IS_NULL:
        if( node.getLeftExpr().getType() == EvalType.FIELD
          && node.getRightExpr().getType() == EvalType.CONST) {
          nodeList.add(node);
        }
      }
    }
  }
}
