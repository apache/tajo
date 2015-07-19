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

import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType.JSON;

public class JsonDataTypeValidator extends DataTypeValidator {
    private static Set<TajoDataTypes.Type> validTypes = new HashSet<TajoDataTypes.Type>(
            Arrays.asList(
                    TajoDataTypes.Type.BOOLEAN,
                    TajoDataTypes.Type.INT1,
                    TajoDataTypes.Type.INT2,
                    TajoDataTypes.Type.INT4,
                    TajoDataTypes.Type.INT8,
                    TajoDataTypes.Type.FLOAT4,
                    TajoDataTypes.Type.FLOAT8,
                    TajoDataTypes.Type.CHAR,
                    TajoDataTypes.Type.TEXT,
                    TajoDataTypes.Type.VARCHAR,
                    TajoDataTypes.Type.INET4,
                    TajoDataTypes.Type.TIMESTAMP,
                    TajoDataTypes.Type.DATE,
                    TajoDataTypes.Type.TIME,
                    TajoDataTypes.Type.INTERVAL,
                    TajoDataTypes.Type.BIT,
                    TajoDataTypes.Type.BINARY,
                    TajoDataTypes.Type.BLOB,
                    TajoDataTypes.Type.VARBINARY,
                    TajoDataTypes.Type.NULL_TYPE
            )
    );

    @Override
    public CatalogProtos.StoreType getStoreType() {
        return JSON;
    }

    @Override
    public Set<TajoDataTypes.Type> getValidTypeSet() {
        return validTypes;
    }
}
