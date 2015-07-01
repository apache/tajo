/*
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

package org.apache.tajo.catalog.exception;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.error.Errors;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.function.FunctionUtil;

import java.util.Collection;

public class CatalogExceptionUtil {

  public static CatalogException makeUndefinedDatabase(String dbName) {
    return new CatalogException(ResultCode.UNDEFINED_DATABASE, dbName);
  }

  public static CatalogException makeUndefinedFunction(String signature) {
    return new CatalogException(ResultCode.UNDEFINED_FUNCTION, signature);
  }

  public static CatalogException makeUndefinedFunction(String funcName, TajoDataTypes.DataType[] parameters) {
    return new CatalogException(ResultCode.UNDEFINED_FUNCTION,
        FunctionUtil.buildSimpleFunctionSignature(funcName, parameters));
  }

  public static CatalogException makeUndefinedFunction(String funcName, Collection<TajoDataTypes.DataType> parameters) {
    return new CatalogException(
        ResultCode.UNDEFINED_FUNCTION, FunctionUtil.buildSimpleFunctionSignature(funcName, parameters));
  }

  public static CatalogException makeUndefinedTable(String tbName) {
    return new CatalogException(ResultCode.UNDEFINED_TABLE, tbName);
  }

  public static CatalogException makeDuplicateTable(String tbName) {
    return new CatalogException(ResultCode.DUPLICATE_TABLE, tbName);
  }

  public static CatalogException makeCatalogUpgrade() {
    return new CatalogException(ResultCode.CAT_UPGRADE_REQUIRED);
  }

  public static CatalogException makeTMCNoMatchedDataType(String dataType) {
    return new CatalogException(ResultCode.TMC_NO_MATCHED_DATATYPE, dataType);
  }
}
