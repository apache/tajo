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

package org.apache.tajo.engine.eval;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.storage.Tuple;

/**
 * An annotated expression which includes actual data domains.
 * It is also used for evaluation.
 */
public abstract class EvalNode implements Cloneable, GsonObject {
	@Expose protected EvalType type;
  @Expose protected DataType returnType = null;

  public EvalNode() {
  }

	public EvalNode(EvalType type) {
		this.type = type;
	}
	
	public EvalType getType() {
		return this.type;
	}
	
	public abstract DataType getValueType();
	
	public abstract String getName();

  @Override
	public String toJson() {
    return CoreGsonHelper.toJson(this, EvalNode.class);
	}
	
	public abstract <T extends Datum> T eval(Schema schema, Tuple tuple);

  @Deprecated
  public abstract  void preOrder(EvalNodeVisitor visitor);

  @Deprecated
  public abstract void postOrder(EvalNodeVisitor visitor);

  @Override
  public Object clone() throws CloneNotSupportedException {
    EvalNode evalNode = (EvalNode) super.clone();
    evalNode.type = type;
    evalNode.returnType = returnType;
    return evalNode;
  }
}
