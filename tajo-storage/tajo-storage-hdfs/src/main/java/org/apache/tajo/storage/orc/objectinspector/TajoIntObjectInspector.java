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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.tajo.datum.Int4Datum;

public class TajoIntObjectInspector extends TajoPrimitiveObjectInspector implements IntObjectInspector {
  @Override
  public int get(Object o) {
    return ((Int4Datum)o).asInt4();
  }

  @Override
  public PrimitiveTypeInfo getTypeInfo() {
    return TypeInfoFactory.intTypeInfo;
  }

  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return PrimitiveCategory.INT;
  }

  @Override
  public Class<?> getPrimitiveWritableClass() {
    return null;
  }

  @Override
  public Object getPrimitiveWritableObject(Object o) {
    return null;
  }

  @Override
  public Class<?> getJavaPrimitiveClass() {
    return Integer.class;
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return null;
  }

  @Override
  public Object copyObject(Object o) {
    return null;
  }

  @Override
  public boolean preferWritable() {
    return false;
  }

  @Override
  public String getTypeName() {
    return "INT4";
  }
}
