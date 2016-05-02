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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.util.StringUtils;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;

/**
 * Represents a qualified identifier. In other words, it represents identifiers for
 * all kinds of of database objects, such as databases, tables, and columns.
 */
public class QualifiedIdentifier {
  private ImmutableList<Identifier> names;

  private QualifiedIdentifier(ImmutableList<Identifier> names) {
    this.names = names;
  }

  /**
   * Return a raw string representation for an identifier, which is equivalent to ones used in SQL statements.
   *
   * @param policy IdentifierPolicy
   * @return Raw identifier string
   */
  public String raw(final IdentifierPolicy policy) {
    return StringUtils.join(names, "" + policy.getIdentifierSeperator(), new Function<Identifier, String>() {
      @Override
      public String apply(@Nullable Identifier identifier) {
        return identifier.raw(policy);
      }
    });
  }

  public String interned() {
    return interned(DefaultPolicy());
  }

  /**
   * Raw string of qualified identifier
   * @param policy Identifier Policy
   * @return raw string
   */
  public String interned(final IdentifierPolicy policy) {
    return StringUtils.join(names, "" + policy.getIdentifierSeperator(), new Function<Identifier, String>() {
      @Override
      public String apply(@Nullable Identifier identifier) {
        return identifier.interned(policy);
      }
    });
  }

  @Override
  public String toString() {
    return interned(DefaultPolicy());
  }

  @Override
  public int hashCode() {
    return Objects.hash(names);
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof QualifiedIdentifier) {
      QualifiedIdentifier other = (QualifiedIdentifier) obj;
      return names.equals(other.names);
    }

    return false;
  }

  public static QualifiedIdentifier $(Collection<Identifier> names) {
    return new QualifiedIdentifier(ImmutableList.copyOf(names));
  }

  public static QualifiedIdentifier $(Identifier...names) {
    return new QualifiedIdentifier(ImmutableList.copyOf(names));
  }

  /**
   * It takes interned strings. It assumes all parameters already stripped. It is used only for tests.
   * @param names interned strings
   * @return QualifiedIdentifier
   */
  @VisibleForTesting
  public static QualifiedIdentifier $(String...names) {
    final ImmutableList.Builder<Identifier> builder = ImmutableList.builder();
    for (String n : names) {
      for (String split : n.split(StringUtils.escapeRegexp(""+DefaultPolicy().getIdentifierSeperator()))) {
        builder.add(Identifier._(split, IdentifierUtil.isShouldBeQuoted(split)));
      }
    }
    return new QualifiedIdentifier(builder.build());
  }
}
