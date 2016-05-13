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
 * ANSI SQL Standard Identifier Policy
 */
public class ANSISQLIdentifierPolicy extends IdentifierPolicy {

  @Override
  String getName() {
    return "ANSI SQL Identifier Policy";
  }

  @Override
  public char getIdentifierQuoteString() {
    return ANSI_SQL_QUOTE_STRING;
  }

  @Override
  char getIdentifierSeperator() {
    return ANSI_SQL_SEPERATOR_STRING;
  }

  @Override
  public int getMaxColumnNameLength() {
    return MAX_IDENTIFIER_LENGTH;
  }

  @Override
  IdentifierCase storesUnquotedIdentifierAs() {
    return IdentifierCase.UpperCase;
  }

  @Override
  IdentifierCase storesQuotedIdentifierAs() {
    return IdentifierCase.MixedCase;
  }
}
