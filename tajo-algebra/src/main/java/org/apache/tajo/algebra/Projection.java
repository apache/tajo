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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import org.apache.tajo.util.TUtil;

public class Projection extends UnaryOperator implements Cloneable {
  private boolean all;
  private boolean distinct = false;

  private NamedExpr[] targets;

  public Projection() {
    super(OpType.Projection);
  }

  public int size() {
    return targets.length;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setDistinct() {
    distinct = true;
  }

  public void setAll() {
    all = true;
  }

  public boolean isAllProjected() {
    return all;
  }
	
	public NamedExpr[] getNamedExprs() {
	  return this.targets;
	}

  public void setNamedExprs(NamedExpr[] targets) {
    this.targets = targets;
  }

  public int hashCode() {
    return Objects.hashCode(all, distinct, targets, getChild());
  }

  @Override
  boolean equalsTo(Expr expr) {
    Projection another = (Projection) expr;
    return TUtil.checkEquals(all, another.all) && distinct == another.distinct &&
        TUtil.checkEquals(targets, another.targets);
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
