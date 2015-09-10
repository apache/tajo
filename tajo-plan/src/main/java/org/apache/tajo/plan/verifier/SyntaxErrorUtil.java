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

package org.apache.tajo.plan.verifier;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.DataTypeMismatchException;
import org.apache.tajo.plan.logical.NodeType;

public class SyntaxErrorUtil {

  public static SyntaxErrorException makeSyntaxError(String message) {
    return new SyntaxErrorException(ResultCode.SYNTAX_ERROR, message);
  }

  public static DataTypeMismatchException makeDataTypeMisMatch(Column src, Column target) {
    return new DataTypeMismatchException(
        src.getSimpleName(), src.getDataType().getType().name(),
        target.getSimpleName(), target.getDataType().getType().name());
  }

  public static SyntaxErrorException makeSetOpDataTypeMisMatch(NodeType type, DataType src, DataType target) {
    return new SyntaxErrorException(ResultCode.SET_OPERATION_DATATYPE_MISMATCH,
        type.name(), src.getType().name(), target.getType().name());
  }

  public static SyntaxErrorException makeDuplicateAlias(String name) {
    return new SyntaxErrorException(ResultCode.DUPLICATE_ALIAS, name);
  }

  public static SyntaxErrorException makeInvalidSessionVar(String varName, String message) {
    return new SyntaxErrorException(ResultCode.INVALID_SESSION_VARIABLE, varName, message);
  }

  public static SyntaxErrorException makeUnknownDataFormat(String dataFormat) {
    return new SyntaxErrorException(ResultCode.UNKNOWN_DATAFORMAT, dataFormat);
  }
}
