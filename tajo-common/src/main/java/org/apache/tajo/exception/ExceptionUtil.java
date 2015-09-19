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

package org.apache.tajo.exception;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.error.Errors;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;

import java.lang.reflect.Constructor;
import java.util.Map;

import static org.apache.tajo.error.Errors.ResultCode.*;
import static org.apache.tajo.exception.ReturnStateUtil.isError;

public class ExceptionUtil {

  static Map<Errors.ResultCode, Class<? extends DefaultTajoException>> EXCEPTIONS = Maps.newHashMap();

  static {

    // General Errors
    ADD_EXCEPTION(INTERNAL_ERROR, TajoInternalError.class);
    ADD_EXCEPTION(FEATURE_NOT_SUPPORTED, UnsupportedException.class);
    ADD_EXCEPTION(NOT_IMPLEMENTED, NotImplementedException.class);

    // Query Management and Scheduler
    ADD_EXCEPTION(QUERY_NOT_FOUND, QueryNotFoundException.class);

    // Syntax Error or Access Rule Violation
    ADD_EXCEPTION(SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, SQLSyntaxError.class);
    ADD_EXCEPTION(SYNTAX_ERROR, SQLSyntaxError.class);
    ADD_EXCEPTION(INSUFFICIENT_PRIVILEGE, InsufficientPrivilegeException.class);

    ADD_EXCEPTION(UNDEFINED_TABLESPACE, UndefinedTablespaceException.class);
    ADD_EXCEPTION(UNDEFINED_DATABASE, UndefinedDatabaseException.class);
    // ADD_EXCEPTION(UNDEFINED_SCHEMA, );
    ADD_EXCEPTION(UNDEFINED_TABLE, UndefinedTableException.class);
    ADD_EXCEPTION(UNDEFINED_COLUMN, UndefinedColumnException.class);
    ADD_EXCEPTION(UNDEFINED_FUNCTION, UndefinedFunctionException.class);
    ADD_EXCEPTION(UNDEFINED_PARTITION, UndefinedPartitionException.class);
    ADD_EXCEPTION(UNDEFINED_PARTITION_KEY, UndefinedPartitionKeyException.class);
    ADD_EXCEPTION(UNDEFINED_OPERATOR, UndefinedOperatorException.class);
    ADD_EXCEPTION(UNDEFINED_INDEX_NAME, UndefinedIndexException.class);

    ADD_EXCEPTION(DUPLICATE_TABLESPACE, DuplicateTablespaceException.class);
    ADD_EXCEPTION(DUPLICATE_DATABASE, DuplicateDatabaseException.class);
    // ADD_EXCEPTION(DUPLICATE_SCHEMA, );
    ADD_EXCEPTION(DUPLICATE_TABLE, DuplicateTableException.class);
    ADD_EXCEPTION(DUPLICATE_COLUMN, DuplicateColumnException.class);
    // ADD_EXCEPTION(DUPLICATE_ALIAS, );
    ADD_EXCEPTION(DUPLICATE_INDEX, DuplicateIndexException.class);
    ADD_EXCEPTION(DUPLICATE_PARTITION, DuplicatePartitionException.class);

    ADD_EXCEPTION(AMBIGUOUS_TABLE, AmbiguousTableException.class);
    ADD_EXCEPTION(AMBIGUOUS_COLUMN, AmbiguousColumnException.class);
    ADD_EXCEPTION(AMBIGUOUS_FUNCTION, AmbiguousFunctionException.class);

    ADD_EXCEPTION(DATATYPE_MISMATCH, DataTypeMismatchException.class);
    ADD_EXCEPTION(DATATYPE_MISMATCH, InvalidValueForCastException.class);

    ADD_EXCEPTION(UNAVAILABLE_TABLE_LOCATION, UnavailableTableLocationException.class);
    ADD_EXCEPTION(UNKNOWN_DATAFORMAT, UnknownDataFormatException.class);
    ADD_EXCEPTION(UNSUPPORTED_DATATYPE, UnsupportedDataTypeException.class);
    ADD_EXCEPTION(INVALID_DATATYPE, InvalidDataTypeException.class);
    ADD_EXCEPTION(INVALID_TABLE_PROPERTY, InvalidTablePropertyException.class);
    ADD_EXCEPTION(MISSING_TABLE_PROPERTY, MissingTablePropertyException.class);

    ADD_EXCEPTION(TOO_LARGE_INPUT_FOR_CROSS_JOIN, TooLargeInputForCrossJoinException.class);
    ADD_EXCEPTION(INVALID_INPUTS_FOR_CROSS_JOIN, InvalidInputsForCrossJoin.class);

    ADD_EXCEPTION(CAT_UNSUPPORTED_CATALOG_STORE, UnsupportedCatalogStore.class);
  }

  private static void ADD_EXCEPTION(Errors.ResultCode code, Class<? extends DefaultTajoException> cls) {
    EXCEPTIONS.put(code, cls);
  }

  /**
   * If the exception is equivalent to the error corresponding to the expected exception, throws the exception.
   * It is used to throw an exception for a error.
   *
   * @param state ReturnState
   * @param clazz Exception class corresponding to the expected
   * @param <T> Exception class
   * @throws T Exception
   */
  public static <T extends TajoException> void throwsIfThisError(ReturnState state, Class<T> clazz) throws T {
    if (isError(state)) {
      T exception = (T) toTajoException(state);
      if (exception.getClass().equals(clazz)) {
        throw exception;
      }
    }
  }

  /**
   * It can throw any TajoException if any error occurs.
   *
   * @param state
   * @throws TajoException
   */
  public static void throwIfError(ReturnState state) throws TajoException {
    if (isError(state)) {
      throw toTajoException(state);
    }
  }

  public static DefaultTajoException toTajoExceptionCommon(ReturnState state) {
    if (state.getReturnCode() == Errors.ResultCode.INTERNAL_ERROR) {
      return new TajoInternalError(state);

    } else if (EXCEPTIONS.containsKey(state.getReturnCode())) {
      Object exception;
      try {
        Class clazz = EXCEPTIONS.get(state.getReturnCode());
        Constructor c = clazz.getConstructor(ReturnState.class);
        exception = c.newInstance(new Object[]{state});
      } catch (Throwable t) {
        throw new TajoInternalError(t);
      }

      if (exception instanceof TajoException) {
        return (TajoException) exception;
      } else if (exception instanceof TajoRuntimeException) {
        return ((TajoRuntimeException) exception);
      } else {
        return ((TajoError) exception);
      }

    } else {
      throw new TajoInternalError(
          "Cannot restore the exception for [" + state.getReturnCode().name() +"] '" + state.getMessage() +"'");
    }
  }

  public static TajoException toTajoException(ReturnState state) throws TajoRuntimeException, TajoError {
    DefaultTajoException e = toTajoExceptionCommon(state);

    if (e instanceof TajoException) {
      return (TajoException) e;
    } else if (e instanceof TajoRuntimeException) {
      throw ((TajoRuntimeException) e);
    } else {
      throw ((TajoError) e);
    }
  }

  /**
   * Determine if a Throwable has Tajo's ReturnCode and error message.
   *
   * @param t Throwable
   * @return true if a Throwable has Tajo's ReturnCode and error message.
   */
  public static boolean isExceptionWithResultCode(Throwable t) {
    return t instanceof DefaultTajoException;
  }

  /**
   * Determine if a Throwable is caused by user's wrong input and invalid data instead of a bug.
   *
   * @param t Throwable
   * @return true if a Throwable has Tajo's ReturnCode and error message.
   */
  public static boolean isManagedException(Throwable t) {
    return t instanceof TajoException || t instanceof TajoRuntimeException;
  }

  private static void printStackTrace(Log log, Throwable t) {
    log.error("\nStack Trace:\n" + StringUtils.stringifyException(t));
  }

  public static void printStackTraceIfError(Log log, Throwable t) {
    if (System.getProperty("DEBUG") != null || !ExceptionUtil.isManagedException(t)) {
      ExceptionUtil.printStackTrace(log, t);
    }
  }

  public static UnsupportedException makeNotSupported(String feature) {
    return new UnsupportedException(feature);
  }

  /**
   * Return the string about the exception line ; e.g.,)
   * <code>Line 195 in JdbcTablespace.java</code>
   *
   * @return A string representing the line number and source file name at which the exception occurs.
   */
  @SuppressWarnings("unused")
  public static String getExceptionLine() {
    StackTraceElement stack = Thread.currentThread().getStackTrace()[3];
    return "Line " + stack.getLineNumber() + " in " + stack.getFileName();
  }

  /**
   * Return the string about the exception point; e.g.,)
   * <code>org.apache.tajo.storage.mysql.JdbcTablespace::createTable</code>
   *
   * @return A string representing the class and method names at which the exception occurs.
   */
  public static String getExceptionPoint() {
    StackTraceElement stack = Thread.currentThread().getStackTrace()[3];
    return stack.getClassName() + "::" + stack.getMethodName();
  }
}
