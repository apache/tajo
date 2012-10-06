/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock.LimitClause;
import tajo.engine.utils.TUtil;

/**
 * @author Hyunsik Choi
 *
 */
public final class LimitNode extends UnaryNode implements Cloneable {
	@Expose
  private LimitClause limitClause;

	public LimitNode() {
		super();
	}

  public LimitNode(LimitClause limitClause) {
    super(ExprType.LIMIT);
    this.limitClause = limitClause;
  }
  
  public long getFetchFirstNum() {
    return this.limitClause.getLimitRow();
  }
  
  @Override 
  public boolean equals(Object obj) {
    if (obj instanceof LimitNode) {
      LimitNode other = (LimitNode) obj;
      return super.equals(other)
          && TUtil.checkEquals(limitClause, other.limitClause)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    LimitNode newLimitNode = (LimitNode) super.clone();
    newLimitNode.limitClause = (LimitClause) limitClause.clone();
    return newLimitNode;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder(limitClause.toString());

    sb.append("]");

    sb.append("\n\"out schema: " + getOutSchema()
        + "\n\"in schema: " + getInSchema());
    return sb.toString()+"\n"
        + getSubNode().toString();
  }
  
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
