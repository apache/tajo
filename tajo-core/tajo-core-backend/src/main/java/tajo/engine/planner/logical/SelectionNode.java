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

package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.engine.eval.EvalNode;
import tajo.engine.json.GsonCreator;

public class SelectionNode extends UnaryNode implements Cloneable {

	@Expose
	private EvalNode qual;
	
	public SelectionNode() {
		super();
	}
	
	public SelectionNode(EvalNode qual) {
		super(ExprType.SELECTION);
		setQual(qual);
	}

	public EvalNode getQual() {
		return this.qual;
	}

	public void setQual(EvalNode qual) {
		this.qual = qual;
	}
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Selection\": {\"qual\": \"").append(qual.toString()).append("\",");
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInSchema()).append("}");
    
    return sb.toString()+"\n"
    + getSubNode().toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionNode) {
      SelectionNode other = (SelectionNode) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SelectionNode selNode = (SelectionNode) super.clone();
    selNode.qual = (EvalNode) this.qual.clone();
    
    return selNode;
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
