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

package org.apache.tajo.plan;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.plan.validator.DataTypeValidator;
import org.apache.tajo.plan.validator.JsonDataTypeValidator;
import org.apache.tajo.plan.validator.ParquetDataTypeValidator;
import org.apache.tajo.plan.verifier.VerificationState;

import java.util.HashMap;
import java.util.Map;

public class DataTypeValidatorFactory {
  static final Map<CatalogProtos.StoreType, DataTypeValidator> validatorMap = new HashMap<CatalogProtos.StoreType, DataTypeValidator>();

  static {
    validatorMap.put(CatalogProtos.StoreType.PARQUET, new ParquetDataTypeValidator());
    validatorMap.put(CatalogProtos.StoreType.JSON, new JsonDataTypeValidator());
  };

  public static void validate(VerificationState state, CatalogProtos.StoreType storeType, Schema scheme) {
    DataTypeValidator validator = validatorMap.get(storeType);

    if (validator != null) {
      validator.validate(state, scheme);
    }
  }
}
