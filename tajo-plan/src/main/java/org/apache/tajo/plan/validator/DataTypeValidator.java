/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.validator;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.plan.verifier.VerificationState;

import java.util.Set;


public abstract class DataTypeValidator {

  public abstract CatalogProtos.StoreType getStoreType();
  public abstract Set<TajoDataTypes.Type> getValidTypeSet();

  public void validate(VerificationState state, Schema schema) {
    int size = schema.size();
    Set<TajoDataTypes.Type> validTypes = getValidTypeSet();

    for (int i = 0; i < size; i++) {
      Column column = schema.getColumn(i);
      TajoDataTypes.Type type = column.getDataType().getType();

      if (validTypes.contains(type) == false) {
        String error = getStoreType().name() + " doesn't support " + column.getQualifiedName() + ":" + type.toString();
        state.addVerification(error);
      }
    }
  }
}
