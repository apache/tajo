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

package org.apache.tajo.plan.expr;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;

public class ConstEval extends EvalNode implements Comparable<ConstEval>, Cloneable {
	@Expose Datum datum = null;
	
	public ConstEval(Datum datum) {
		super(EvalType.CONST);
		this.datum = datum;
	}

  public Datum getValue() {
    return this.datum;
  }

  @Override
	public String toString() {
		return datum.toString();
	}

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    return datum;
  }

  @Override
	public DataType getValueType() {
    return CatalogUtil.newSimpleDataType(datum.type());
	}

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public EvalNode getChild(int idx) {
    return null;
  }

  @Override
	public String getName() {
		return this.datum.toString();
	}
	
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConstEval) {
      ConstEval other = (ConstEval) obj;

      if (this.type == other.type && this.datum.equals(other.datum)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(type, datum.type(), datum);
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    ConstEval eval = (ConstEval) super.clone();
    eval.datum = datum;
    
    return eval;
  }

  @Override
  public int compareTo(ConstEval other) {    
    return datum.compareTo(other.datum);
  }
  
  @Override
  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
}
