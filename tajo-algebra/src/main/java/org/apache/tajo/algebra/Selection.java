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
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

public class Selection extends UnaryOperator implements JsonSerializable {
  @Expose @SerializedName("SelectCondition")
  private Expr qual;

  public Selection(Expr qual) {
    super(OpType.Filter);
    this.qual = qual;
  }

  public boolean hasQual() {
    return qual != null;
  }

  public void setQual(Expr expr) {
    this.qual = expr;
  }

	public Expr getQual() {
		return this.qual;
	}

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public int hashCode() {
    return Objects.hashCode(qual, getChild());
  }

  @Override
  public boolean equalsTo(Expr expr) {
    if (expr instanceof Selection) {
      Selection other = (Selection) expr;
      return TUtil.checkEquals(qual, other.qual);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Selection selection = (Selection) super.clone();
    selection.qual = (Expr) qual.clone();
    return selection;
  }
}
