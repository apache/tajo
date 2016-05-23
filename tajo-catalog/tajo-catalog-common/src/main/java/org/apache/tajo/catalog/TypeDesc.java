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

package org.apache.tajo.catalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.TUtil;

/**
 * Type Description for a column
 */
public class TypeDesc {
  @Expose protected DataType dataType;
  @Expose protected Schema nestedRecordSchema; // NULL unless type is RECORD.

  public TypeDesc(DataType dataType) {
    this.dataType = dataType;
  }

  public TypeDesc(Schema recordSchema) {
    this.dataType = CatalogUtil.newSimpleDataType(Type.RECORD);
    this.nestedRecordSchema = recordSchema;
  }

  public DataType getDataType() {
    return dataType;
  }

  public boolean equals(Object obj) {
    if (obj instanceof TypeDesc) {
      TypeDesc other = (TypeDesc) obj;
      return  this.dataType.equals(other.dataType) &&
              TUtil.checkEquals(nestedRecordSchema, other.nestedRecordSchema);

    } else {
      return false;
    }
  }

  public Schema getNestedSchema() {
    return nestedRecordSchema;
  }

  public int hashCode() {
    return Objects.hashCode(dataType.hashCode(), nestedRecordSchema);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();

    if (dataType.getType() == Type.RECORD) {
      sb.append("RECORD").append(SchemaUtil.toDisplayString(nestedRecordSchema)).append("");
    } else {
      sb.append(dataType.getType().name());
      if (dataType.getLength() > 0) {
        sb.append("(" + dataType.getLength() + ")");
      }
    }
    return sb.toString();
  }
}
