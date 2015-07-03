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

package org.apache.tajo.storage.orc.objectinspector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TajoStructObjectInspector extends StructObjectInspector {
  private final static Log LOG = LogFactory.getLog(TajoStructObjectInspector.class);
  private Schema schema;
  private List<TajoStructField> structFields;

  static class TajoStructField implements StructField {
    private String name;
    private ObjectInspector oi;
    private String comment;

    TajoStructField(String name, ObjectInspector oi) {
      this(name, oi, null);
    }

    TajoStructField(String name, ObjectInspector oi, String comment) {
      this.name = name;
      this.oi = oi;
      this.comment = comment;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String getFieldComment() {
      return comment;
    }
  }

  TajoStructObjectInspector(Schema schema) {
    this.schema = schema;
    structFields = new ArrayList<TajoStructField>(schema.size());

    for (Column c: schema.getColumns()) {
      try {
        TajoStructField field = new TajoStructField(c.getSimpleName(),
          ObjectInspectorFactory.buildObjectInspectorByType(c.getDataType().getType()));
        structFields.add(field);
      } catch (UnsupportedException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public StructField getStructFieldRef(String s) {
    for (TajoStructField field:structFields) {
      if (field.getFieldName().equals(s)) {
        return field;
      }
    }

    return null;
  }

  @Override
  public Object getStructFieldData(Object o, StructField structField) {
    return null;
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object o) {
    return null;
  }

  @Override
  public String getTypeName() {
    return "struct";
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }
}
