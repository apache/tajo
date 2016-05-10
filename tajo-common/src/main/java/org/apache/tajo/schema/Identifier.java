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

import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.IdentifierProto;
import org.apache.tajo.schema.IdentifierPolicy.IdentifierCase;

import java.util.Objects;

import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;

/**
 * Represents an identifier part
 */
public class Identifier implements ProtoObject<IdentifierProto> {
  private String name;
  private boolean quoted;

  /**
   * Identifier constructor
   * @param name Identifier part string
   * @param quoted quoted or not
   */
  private Identifier(String name, boolean quoted) {
    this.name = name;
    this.quoted = quoted;
  }

  public static Identifier _(String name) {
    return new Identifier(name, false);
  }

  public static Identifier _(String name, boolean quoted) {
    return new Identifier(name, quoted);
  }

  /**
   * Raw string for an identifier, which is equivalent to an identifier directly used in SQL statements.
   * @param policy IdentifierPolicy
   * @return Raw string
   */
  public String raw(IdentifierPolicy policy) {
    StringBuilder sb = new StringBuilder();
    if (quoted) {
      appendByCase(sb, policy.storesQuotedIdentifierAs());
      sb.insert(0, policy.getIdentifierQuoteString());
      sb.append(policy.getIdentifierQuoteString());
    } else {
      appendByCase(sb, policy.storesUnquotedIdentifierAs());
    }

    return sb.toString();
  }

  /**
   * Interned string of an identifier
   * @param policy IdentifierPolicy
   * @return interned string
   */
  public String interned(IdentifierPolicy policy) {
    StringBuilder sb = new StringBuilder();
    if (quoted) {
      appendByCase(sb, policy.storesQuotedIdentifierAs());
    } else {
      appendByCase(sb, policy.storesUnquotedIdentifierAs());
    }

    return sb.toString();
  }

  private void appendByCase(StringBuilder sb, IdentifierCase c) {
    switch (c) {
    case LowerCase:
      sb.append(name.toLowerCase());
      break;
    case UpperCase:
      sb.append(name.toUpperCase());
      break;
    case MixedCase:
      sb.append(name);
      break;
    }
  }

  @Override
  public String toString() {
    return raw(DefaultPolicy());
  }

  public int hashCode() {
    return Objects.hash(name, quoted);
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Identifier) {
      Identifier other = (Identifier) obj;
      return name.equals(other.name) && quoted == other.quoted;
    }

    return false;
  }

  @Override
  public IdentifierProto getProto() {
    return IdentifierProto.newBuilder()
        .setName(name)
        .setQuoted(quoted)
        .build();
  }

  public static Identifier fromProto(IdentifierProto proto) {
    return new Identifier(proto.getName(), proto.getQuoted());
  }
}
