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

package org.apache.tajo.client;

import com.google.common.collect.Maps;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.*;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

import java.lang.reflect.Constructor;
import java.util.Map;

import static org.apache.tajo.error.Errors.ResultCode.*;
import static org.apache.tajo.exception.ReturnStateUtil.isError;

/**
 * Exception related utilities. Especially, it provides a way to recover @{link ReturnState} into TajoException.
 */
public class ClientExceptionUtil {

  static Map<ResultCode, Class<? extends TajoExceptionInterface>> EXCEPTIONS = Maps.newHashMap();

  static {

    // General Errors
    ADD_EXCEPTION(INTERNAL_ERROR, TajoInternalError.class);

    ADD_EXCEPTION(UNDEFINED_TABLESPACE, UndefinedTablespaceException.class);
    ADD_EXCEPTION(UNDEFINED_DATABASE, UndefinedDatabaseException.class);
    // ADD_EXCEPTION(UNDEFINED_SCHEMA, );
    ADD_EXCEPTION(UNDEFINED_TABLE, UndefinedTableException.class);
    ADD_EXCEPTION(UNDEFINED_COLUMN, UndefinedColumnException.class);
    ADD_EXCEPTION(UNDEFINED_FUNCTION, UndefinedFunctionException.class);
    ADD_EXCEPTION(UNDEFINED_PARTITION, UndefinedPartitionException.class);
    ADD_EXCEPTION(UNDEFINED_OPERATOR, UndefinedOperatorException.class);

    ADD_EXCEPTION(DUPLICATE_TABLESPACE, DuplicateTableException.class);
    ADD_EXCEPTION(DUPLICATE_DATABASE, DuplicateDatabaseException.class);
    // ADD_EXCEPTION(DUPLICATE_SCHEMA, );
    ADD_EXCEPTION(DUPLICATE_TABLE, DuplicateTableException.class);
    ADD_EXCEPTION(DUPLICATE_COLUMN, DuplicateColumnException.class);
    // ADD_EXCEPTION(DUPLICATE_ALIAS, );
    ADD_EXCEPTION(DUPLICATE_INDEX, DuplicateIndexException.class);
    ADD_EXCEPTION(DUPLICATE_PARTITION, DuplicatePartitionException.class);

    ADD_EXCEPTION(AMBIGUOUS_TABLE, AmbiguousTableException.class);
    ADD_EXCEPTION(AMBIGUOUS_COLUMN, AmbiguousColumnException.class);
  }

  private static void ADD_EXCEPTION(ResultCode code, Class<? extends TajoExceptionInterface> cls) {
    EXCEPTIONS.put(code, cls);
  }

  public static void throwIfError(ReturnState state) throws TajoException {
    if (isError(state)) {
      throw toTajoException(state);
    }
  }

  public static TajoException toTajoException(ReturnState state) {

    if (state.getReturnCode() == ResultCode.INTERNAL_ERROR) {
      throw new TajoInternalError(state);

    } else if (EXCEPTIONS.containsKey(state.getReturnCode())) {
      try {
        Constructor c = EXCEPTIONS.get(state.getReturnCode()).getConstructor(ReturnState.class);
        return (TajoException) c.newInstance(new Object[] {state});
      } catch (Throwable t) {
        throw new TajoInternalError(t);
      }
    } else {
      throw new TajoInternalError("Unregistred Exception (" + state.getReturnCode().name() +"): "
          + state.getMessage());
    }
  }
}
