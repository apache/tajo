/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.algebra;

import tajo.util.TUtil;

public class Selection extends UnaryOperator implements JsonSerializable {
  private Expr search_condition;

  public Selection(Expr relation, Expr qual) {
    super(ExprType.Selection);
    setChild(relation);
    search_condition = qual;
  }

  public boolean hasQual() {
    return search_condition != null;
  }

  public void setQual(Expr expr) {
    this.search_condition = expr;
  }

	public Expr getQual() {
		return this.search_condition;
	}

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    if (expr instanceof Selection) {
      Selection other = (Selection) expr;
      return TUtil.checkEquals(search_condition, other.search_condition);
    }
    return false;
  }
}
