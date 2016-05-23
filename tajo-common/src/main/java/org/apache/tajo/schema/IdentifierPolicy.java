/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.schema;

/**
 * Policy to describe how to deal identifiers
 */
public abstract class IdentifierPolicy {

  /** Quote String; e.g., 'abc' */
  public static final char ANSI_SQL_QUOTE_STRING = '\'';
  /** Separator; e.g., abc.xyz */
  public static final char ANSI_SQL_SEPERATOR_STRING = '.';
  /** Maximum length of identifiers */
  public final static int MAX_IDENTIFIER_LENGTH = 128;

  public enum IdentifierCase {
    LowerCase,
    UpperCase,
    MixedCase
  }

  /**
   * Policy name
   *
   * @return Policy name
   */
  abstract String getName();

  /**
   * Retrieves the string used to quote SQL identifiers. This method returns a space " "
   * if identifier quoting is not supported.
   *
   * @return the quoting string or a space if quoting is not supported
   */
  abstract char getIdentifierQuoteString();

  /**
   * Retrieves the <code>String</code> that this policy uses as the separator between
   * identifiers.
   *
   * @return the separator string
   */
  abstract char getIdentifierSeperator();

  /**
   * Retrieves the maximum number of characters this policy allows for a column name.
   *
   * @return the maximum number of characters allowed for a column name;
   *      a result of zero means that there is no limit or the limit is not known
   */
  abstract int getMaxColumnNameLength();

  /**
   * Retrieves whether this policy treats unquoted SQL identifiers as
   * which case and stores them in which case.
   *
   * @return IdentifierCase
   */
  abstract IdentifierCase storesUnquotedIdentifierAs();

  /**
   * Retrieves whether this policy treats quoted SQL identifiers as
   * which case and stores them in which case.
   *
   * @return IdentifierCase
   */
  abstract IdentifierCase storesQuotedIdentifierAs();

  public static final IdentifierPolicy DefaultPolicy() {
    return new TajoIdentifierPolicy();
  }

  public static final IdentifierPolicy ANSISQLPolicy() {
    return new ANSISQLIdentifierPolicy();
  }
}
