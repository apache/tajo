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
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.util.StringUtils;

import javax.annotation.Nullable;
import java.util.Collection;

import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;

public class QualifiedIdentifier {
  private ImmutableList<Identifier> names;

  private QualifiedIdentifier(ImmutableList<Identifier> names) {
    this.names = names;
  }

  public String displayString(final IdentifierPolicy policy) {
    return StringUtils.join(names, policy.getIdentifierSeperator(), new Function<Identifier, String>() {
      @Override
      public String apply(@Nullable Identifier identifier) {
        return identifier.displayString(policy);
      }
    });
  }

  /**
   * Raw string of qualified identifier
   * @param policy Identifier Policy
   * @return raw string
   */
  public String raw(final IdentifierPolicy policy) {
    return StringUtils.join(names, policy.getIdentifierSeperator(), new Function<Identifier, String>() {
      @Override
      public String apply(@Nullable Identifier identifier) {
        return identifier.raw(policy);
      }
    });
  }

  @Override
  public String toString() {
    return displayString(DefaultPolicy());
  }

  @Override
  public int hashCode() {
    return names.hashCode();
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

  @VisibleForTesting
  public static QualifiedIdentifier $(String...names) {
    ImmutableList.Builder<Identifier> builder = new ImmutableList.Builder();
    for (String n :names) {
      builder.add(Identifier._(n));
    }
    return new QualifiedIdentifier(builder.build());
  }
}
