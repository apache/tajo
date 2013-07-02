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

import com.google.gson.*;

import java.lang.reflect.Type;

public abstract class Expr implements JsonSerializable {
  protected ExprType op_type;

	public Expr(ExprType op_type) {
		this.op_type = op_type;
	}
	
	public ExprType getType() {
		return this.op_type;
	}

  abstract boolean equalsTo(Expr expr);

	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof Expr) {
	    Expr casted = (Expr) obj;

      if (this.op_type == casted.op_type && equalsTo(casted)) {
        if (this instanceof UnaryOperator) {
          UnaryOperator one = (UnaryOperator) this;
          UnaryOperator another = (UnaryOperator) casted;
          return one.getChild().equals(another.getChild());
        } else if (this instanceof BinaryOperator) {

          BinaryOperator bin = (BinaryOperator) this;
          BinaryOperator anotherBin = (BinaryOperator) casted;

          if (!bin.getLeft().equals(anotherBin.getLeft())) {
            return false;
          }
          if (!bin.getRight().equals(anotherBin.getRight())) {
            return false;
          }

          return true;
        } else {
          return this.equalsTo(casted);
        }
      }
	  }

    return false;
	}

  @Override
  public String toString() {
    return toJson();
  }

  public String toJson() {
    return JsonHelper.toJson(this);
  }

  static class JsonSerDer
      implements JsonSerializer<Expr>, JsonDeserializer<Expr> {

    @Override
    public Expr deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      String operator = jsonObject.get("type").getAsString();
      return context.deserialize(json, ExprType.valueOf(operator).getBaseClass());
    }


    @Override
    public JsonElement serialize(Expr src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return context.serialize(src, src.op_type.getBaseClass());
    }
  }
}
