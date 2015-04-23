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

package org.apache.tajo.function;

import com.google.gson.annotations.Expose;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.TUtil;

import java.util.Arrays;

/**
 * {@link IntermFunctionSignature} is used for only aggregation functions to keep the schema of intermediate results.
 */
public class IntermFunctionSignature implements ProtoObject<CatalogProtos.IntermFunctionSignatureProto>, Cloneable {

  @Expose private TajoDataTypes.DataType[] intermParamTypes;

  public IntermFunctionSignature(@NotNull TajoDataTypes.DataType[] intermParamTypes) {
    this.intermParamTypes = intermParamTypes;
  }

  public IntermFunctionSignature(CatalogProtos.IntermFunctionSignatureProto proto) {
    this.intermParamTypes = proto.getIntermParamTypesList().toArray(
        new TajoDataTypes.DataType[proto.getIntermParamTypesCount()]);
  }

  public TajoDataTypes.DataType[] getIntermSchema() {
    return intermParamTypes;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(intermParamTypes);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof IntermFunctionSignature) {
      IntermFunctionSignature other = (IntermFunctionSignature) o;
      return TUtil.checkEquals(intermParamTypes, other.intermParamTypes);
    }
    return false;
  }

  @Override
  public String toString() {
    return "intermediate types (" + TUtil.arrayToString(intermParamTypes) + ")";
  }

  @Override
  public CatalogProtos.IntermFunctionSignatureProto getProto() {
    return CatalogProtos.IntermFunctionSignatureProto.newBuilder().addAllIntermParamTypes(
        TUtil.newList(intermParamTypes)).build();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    IntermFunctionSignature clone = (IntermFunctionSignature) super.clone();
    clone.intermParamTypes = intermParamTypes;
    return clone;
  }
}
