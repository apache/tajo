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

/**
 * 
 */
package tajo.engine.eval;

import com.google.gson.annotations.Expose;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.SchemaUtil;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.BoolDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.StringDatum;
import tajo.storage.Tuple;

import java.util.regex.Pattern;

/**
 * @author Hyunsik Choi
 */
public class LikeEval extends BinaryEval {
  @Expose private boolean not;
  @Expose private Column column;
  @Expose private String pattern;
  private static final DataType [] RES_TYPE = SchemaUtil.newNoNameSchema(DataType.BOOLEAN);

  // temporal variables
  private Integer fieldId = null;
  private Pattern compiled;
  private BoolDatum result;

  
  public LikeEval(boolean not, FieldEval field, ConstEval pattern) {
    super(Type.LIKE, field, pattern);
    this.not = not;
    this.column = field.getColumnRef();
    this.pattern = pattern.getValue().asChars();
  }
  
  public void compile(String pattern) {
    String regex = pattern.replace("?", ".");
    regex = regex.replace("%", ".*");
    
    this.compiled = Pattern.compile(regex,
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    result = DatumFactory.createBool(false);
  }

  @Override
  public DataType [] getValueType() {
    return RES_TYPE;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    if (fieldId == null) {
      fieldId = schema.getColumnId(column.getQualifiedName());
      compile(this.pattern);
    }    
    StringDatum str = tuple.getString(fieldId);
    if (not) {
      result.setValue(!compiled.matcher(str.asChars()).matches());      
    } else {
      result.setValue(compiled.matcher(str.asChars()).matches());
    }
  }

  public Datum terminate(EvalContext ctx) {
    return result;
  }
  
  @Override
  public String toString() {
    return this.column + " like '" + pattern +"'";
  }
}