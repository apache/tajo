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

package org.apache.tajo.plan.rewrite.rules;

import com.google.common.base.Function;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.serder.EvalNodeDeserializer;
import org.apache.tajo.plan.serder.EvalNodeSerializer;
import org.apache.tajo.plan.serder.PlanProto.SimplePredicateProto;

import javax.annotation.Nullable;
import java.net.URI;

public class IndexScanInfo extends AccessPathInfo {

  /**
   * Simple predicate represents an equal eval expression which consists of
   * a column and a value.
   */
  // TODO: extend to represent more complex expressions
  public static class SimplePredicate implements ProtoObject<SimplePredicateProto> {
    @Expose private SortSpec keySortSpec;
    @Expose private Datum value;

    public SimplePredicate(SortSpec keySortSpec, Datum value) {
      this.keySortSpec = keySortSpec;
      this.value = value;
    }

    public SimplePredicate(SimplePredicateProto proto) {
      keySortSpec = new SortSpec(proto.getKeySortSpec());
      value = EvalNodeDeserializer.deserialize(proto.getValue());
    }

    public SortSpec getKeySortSpec() {
      return keySortSpec;
    }

    public Datum getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SimplePredicate) {
        SimplePredicate other = (SimplePredicate) o;
        return this.keySortSpec.equals(other.keySortSpec) && this.value.equals(other.value);
      } else {
        return false;
      }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      SimplePredicate clone = new SimplePredicate(this.keySortSpec, this.value);
      return clone;
    }

    @Override
    public SimplePredicateProto getProto() {
      SimplePredicateProto.Builder builder = SimplePredicateProto.newBuilder();
      builder.setKeySortSpec(keySortSpec.getProto());
      builder.setValue(EvalNodeSerializer.serialize(value));
      return builder.build();
    }
  }

  private final URI indexPath;
  private final Schema keySchema;
  private final SimplePredicate[] predicates;

  public IndexScanInfo(TableStats tableStats, IndexDesc indexDesc, SimplePredicate[] predicates) {
    super(ScanTypeControl.INDEX_SCAN, tableStats);
    this.indexPath = indexDesc.getIndexPath();
    this.predicates = predicates;
    keySchema = SchemaBuilder.builder().addAll(predicates, new Function<SimplePredicate, Column>() {
      @Override
      public Column apply(@Nullable SimplePredicate p) {
        return p.getKeySortSpec().getSortKey();
      }
    }).build();
  }

  public URI getIndexPath() {
    return indexPath;
  }

  public Schema getKeySchema() {
    return keySchema;
  }

  public SimplePredicate[] getPredicates() {
    return predicates;
  }
}
