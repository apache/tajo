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

package org.apache.tajo.plan.util;

import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.expr.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

public class IndexUtil {
  
  public static String getIndexName(String indexName , SortSpec[] keys) {
    StringBuilder builder = new StringBuilder();
    builder.append(indexName + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getSortKey().getSimpleName() + "_");
    }
    return builder.toString();
  }

  public static List<EvalNode> getAllEqualEvals(EvalNode qual) {
    EvalTreeUtil.EvalFinder finder = new EvalTreeUtil.EvalFinder(EvalType.EQUAL);
    finder.visitChild(null, qual, new Stack<EvalNode>());
    return finder.getEvalNodes();
  }
  
  private static class FieldAndValueFinder implements EvalNodeVisitor {
    private LinkedList<BinaryEval> nodeList = new LinkedList<BinaryEval>();
    
    public LinkedList<BinaryEval> getNodeList () {
      return this.nodeList;
    }
    
    @Override
    public void visit(EvalNode node) {
      BinaryEval binaryEval = (BinaryEval) node;
      switch(node.getType()) {
      case AND:
        break;
      case EQUAL:
        if( binaryEval.getLeftExpr().getType() == EvalType.FIELD
          && binaryEval.getRightExpr().getType() == EvalType.CONST ) {
          nodeList.add(binaryEval);
        }
        break;
      case IS_NULL:
        if( binaryEval.getLeftExpr().getType() == EvalType.FIELD
          && binaryEval.getRightExpr().getType() == EvalType.CONST) {
          nodeList.add(binaryEval);
        }
      }
    }
  }
}
