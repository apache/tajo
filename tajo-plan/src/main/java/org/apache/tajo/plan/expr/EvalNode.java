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

import com.google.gson.annotations.Expose;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.storage.Tuple;

/**
 * An annotated expression which includes actual data domains.
 * It is also used for evaluation.
 */
public abstract class EvalNode implements Cloneable, GsonObject, ProtoObject<PlanProto.EvalNodeTree> {
  @Expose
  protected EvalType type;
  protected transient boolean isBound;

  public EvalNode() {
  }

  public EvalNode(EvalType type) {
    this.type = type;
  }

  public EvalType getType() {
    return this.type;
  }

  public abstract DataType getValueType();

  public abstract int childNum();

  public abstract EvalNode getChild(int idx);

  public abstract String getName();

  @Override
  public String toJson() {
    return PlanGsonHelper.toJson(this, EvalNode.class);
  }

  public EvalNode bind(@Nullable EvalContext evalContext, Schema schema) {
    for (int i = 0; i < childNum(); i++) {
      getChild(i).bind(evalContext, schema);
    }
    isBound = true;
    return this;
  }

  public <T extends Datum> T eval(Tuple tuple) {
    if (!isBound) {
      throw new IllegalStateException("bind() must be called before eval()");
    }
    return null;
  }

  @Deprecated
  public abstract void preOrder(EvalNodeVisitor visitor);

  @Deprecated
  public abstract void postOrder(EvalNodeVisitor visitor);

  @Override
  public Object clone() throws CloneNotSupportedException {
    EvalNode evalNode = (EvalNode) super.clone();
    evalNode.type = type;
    evalNode.isBound = isBound;
    return evalNode;
  }

  @Override
  public PlanProto.EvalNodeTree getProto() {
    return EvalNodeSerializer.serialize(this);
  }
}
