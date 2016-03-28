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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.util.StringUtils;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;

public class QualifiedIdentifier {
  private List<Identifier> names;

  private QualifiedIdentifier(ImmutableList<Identifier> names) {
    this.names = names;
  }

  public String displayString(final IdentifierPolicy policy) {
    return StringUtils.join(names, ".", new Function<Identifier, String>() {
      @Override
      public String apply(@Nullable Identifier identifier) {
        return identifier.displayString(policy);
      }
    });
  }

  public String toString() {
    return displayString(DefaultPolicy());
  }

  public static QualifiedIdentifier QualifiedIdentifier(Collection<Identifier> names) {
    return new QualifiedIdentifier(ImmutableList.copyOf(names));
  }

  public static QualifiedIdentifier QualifiedIdentifier(Identifier...names) {
    return new QualifiedIdentifier(ImmutableList.copyOf(names));
  }
}
