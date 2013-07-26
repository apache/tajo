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
  protected OpType opType;

	public Expr(OpType opType) {
		this.opType = opType;
	}
	
	public OpType getType() {
		return this.opType;
	}

  abstract boolean equalsTo(Expr expr);

	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof Expr) {
	    Expr casted = (Expr) obj;

      if (this.opType == casted.opType && equalsTo(casted)) {
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

  /**
   * This method provides a visiting way in the post order.
   *
   * @param visitor
   */
  public void accept(ExprVisitor visitor) {
    if (this instanceof UnaryOperator) {
      UnaryOperator unary = (UnaryOperator) this;
      unary.getChild().accept(visitor);
    } else if (this instanceof BinaryOperator) {
      BinaryOperator bin = (BinaryOperator) this;
      bin.getLeft().accept(visitor);
      bin.getRight().accept(visitor);
    }

    visitor.visit(this);
  }

  static class JsonSerDer
      implements JsonSerializer<Expr>, JsonDeserializer<Expr> {

    @Override
    public Expr deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      String operator = jsonObject.get("type").getAsString();
      return context.deserialize(json, OpType.valueOf(operator).getBaseClass());
    }


    @Override
    public JsonElement serialize(Expr src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return context.serialize(src, src.opType.getBaseClass());
    }
  }
}
