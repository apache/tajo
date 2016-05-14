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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FieldConverter;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TypeConverter;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.schema.Field;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.type.Type;

import static org.apache.tajo.schema.Field.Field;
import static org.apache.tajo.schema.QualifiedIdentifier.$;

public class FieldEval extends EvalNode implements Cloneable {
	private Field field;
	private int fieldId = -1;
	
	public FieldEval(String columnName, DataType type) {
		super(EvalType.FIELD);
		this.field = Field($(columnName), TypeConverter.convert(type));
	}

  public FieldEval(String columnName, Type type) {
    super(EvalType.FIELD);
    this.field = Field($(columnName), type);
  }
	
	public FieldEval(Column field) {
	  super(EvalType.FIELD);
	  this.field = FieldConverter.convert(field);
	}

  @Override
  public EvalNode bind(EvalContext evalContext, Schema schema) {
    super.bind(evalContext, schema);
    // TODO - column namespace should be improved to simplify name handling and resolving.

    fieldId = schema.getColumnId(field.name().interned());

    if (fieldId == -1) { // fallback
      fieldId = schema.getColumnIdByName(field.name().interned());
    }

    if (fieldId == -1) {
      throw new IllegalStateException("No Such Column Reference: " + field + ", schema: " + schema);
    }
    return this;
  }

	@Override
  @SuppressWarnings("unchecked")
	public Datum eval(Tuple tuple) {
    super.eval(tuple);
	  return tuple.asDatum(fieldId);
  }

  @Override
	public Type getValueType() {
		return field.type();
	}

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public EvalNode getChild(int idx) {
    return null;
  }

  public Column getColumnRef() {
    return FieldConverter.convert(field);
  }
	
	public String getQualifier() {
	  return IdentifierUtil.extractQualifier(field.name().interned());
	}
	
	public String getColumnName() {
    return IdentifierUtil.extractSimpleName(field.name().interned());
	}
	
	public void replaceColumnRef(String columnName) {
	  this.field = Field(columnName, this.field.type());
	}

	@Override
	public String getName() {
		return this.field.name().interned();
	}

	public String toString() {
	  return this.field.toString();
	}
	
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    return obj instanceof FieldEval && field.equals(((FieldEval) obj).field);
  }
  
  @Override
  public int hashCode() {
    return field.hashCode();
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    FieldEval eval = (FieldEval) super.clone();
    eval.field = this.field;
    eval.fieldId = fieldId;
    
    return eval;
  }

  public void preOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  public void postOrder(EvalNodeVisitor visitor) {
    visitor.visit(this);
  }
}