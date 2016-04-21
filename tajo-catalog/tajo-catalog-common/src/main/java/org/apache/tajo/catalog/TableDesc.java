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

package org.apache.tajo.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import java.net.URI;

public class TableDesc implements ProtoObject<TableDescProto>, GsonObject, Cloneable {
	@Expose protected String tableName;                        // required
  @Expose protected Schema schema;                           // optional for self-describing tables
  @Expose protected TableMeta meta;                          // required
  /** uri is set if external flag is TRUE. */
  @Expose protected URI uri;                                 // required
  @Expose	protected TableStats stats;                        // optional
  /** the description of table partition */
  @Expose protected PartitionMethodDesc partitionMethodDesc; // optional
  /** True if it is an external table. False if it is a managed table. */
  @Expose protected Boolean external;                        // optional

  @VisibleForTesting
	public TableDesc() {
	}

  public TableDesc(String tableName, @Nullable Schema schema, TableMeta meta,
                   @Nullable URI uri, boolean external) {
    this.tableName = tableName;
    this.schema = schema;
    this.meta = meta;
    this.uri = uri;
    this.external = external;
  }

	public TableDesc(String tableName, @Nullable Schema schema, TableMeta meta, @Nullable URI path) {
		this(tableName, schema, meta, path, true);
	}
	
	public TableDesc(String tableName, @Nullable Schema schema, String dataFormat, KeyValueSet options,
                   @Nullable URI path) {
	  this(tableName, schema, new TableMeta(dataFormat, options), path);
	}
	
	public TableDesc(TableDescProto proto) {
	  this(proto.getTableName(), proto.hasSchema() ? SchemaFactory.newV1(proto.getSchema()) : null,
        new TableMeta(proto.getMeta()), proto.hasPath() ? URI.create(proto.getPath()) : null, proto.getIsExternal());
    if(proto.hasStats()) {
      this.stats = new TableStats(proto.getStats());
    }
    if (proto.hasPartition()) {
      this.partitionMethodDesc = new PartitionMethodDesc(proto.getPartition());
    }
	}
	
	public void setName(String tableId) {
		this.tableName = tableId;
	}
	
  public String getName() {
    return this.tableName;
  }
	
	public void setUri(URI uri) {
		this.uri = uri;
	}
	
  public URI getUri() {
    return this.uri;
  }

  public void setMeta(TableMeta info) {
    this.meta = info;
  }
	
	public TableMeta getMeta() {
	  return this.meta;
	}

  public void setSchema(Schema schem) {
    this.schema = schem;
  }

  public boolean hasSchema() {
    return schema != null;
  }

  public boolean hasEmptySchema() {
    return schema.size() == 0;
  }
	
  public Schema getSchema() {
    return schema;
  }

  public Schema getLogicalSchema() {
    if (hasPartition()) {
      Schema logicalSchema = SchemaUtil.merge(schema, getPartitionMethod().getExpressionSchema());
      logicalSchema.setQualifier(tableName);
      return logicalSchema;
    } else {
      return schema;
    }
  }

  public void setStats(TableStats stats) {
    this.stats = stats;
  }

  public boolean hasStats() {
    return this.stats != null;
  }

  public TableStats getStats() {
    return this.stats;
  }

  public boolean hasPartition() {
    return this.partitionMethodDesc != null;
  }

  public PartitionMethodDesc getPartitionMethod() {
    return partitionMethodDesc;
  }

  public void setPartitionMethod(PartitionMethodDesc partitionMethodDesc) {
    this.partitionMethodDesc = partitionMethodDesc;
  }

  public void setExternal(boolean external) {
    this.external = external;
  }

  public boolean isExternal() {
    return external;
  }

  public int hashCode() {
    return Objects.hashCode(tableName, schema, meta, uri, stats, partitionMethodDesc);
  }

  public boolean equals(Object object) {
    if(object instanceof TableDesc) {
      TableDesc other = (TableDesc) object;
      
      boolean eq = tableName.equals(other.tableName);
      eq = eq && schema.equals(other.schema);
      eq = eq && meta.equals(other.meta);
      eq = eq && TUtil.checkEquals(uri, other.uri);
      eq = eq && TUtil.checkEquals(partitionMethodDesc, other.partitionMethodDesc);
      eq = eq && TUtil.checkEquals(external, other.external);
      eq = eq && TUtil.checkEquals(stats, other.stats);
      return eq;
    }
    
    return false;   
  }
	
	public Object clone() throws CloneNotSupportedException {
	  TableDesc desc = (TableDesc) super.clone();
	  desc.tableName = tableName;
    desc.schema = schema != null ? (Schema) schema.clone() : null;
    desc.meta = (TableMeta) meta.clone();
    desc.uri = uri;
    desc.stats = stats != null ? (TableStats) stats.clone() : null;
    desc.partitionMethodDesc = partitionMethodDesc != null ? (PartitionMethodDesc) partitionMethodDesc.clone() : null;
    desc.external = external != null ? external : null;
	  return desc;
	}

  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }
	
	public String toJson() {
		return CatalogGsonHelper.toJson(this, TableDesc.class);
	}

  public TableDescProto getProto() {
    TableDescProto.Builder builder = TableDescProto.newBuilder();

    if (this.tableName != null) {
      builder.setTableName(this.tableName);
    }
    if (this.schema != null) {
      builder.setSchema(schema.getProto());
    }
    if (this.meta != null) {
      builder.setMeta(meta.getProto());
    }
    if (this.uri != null) {
      builder.setPath(this.uri.toString());
    }
    if (this.stats != null) {
      builder.setStats(this.stats.getProto());
    }
    if (this.partitionMethodDesc != null) {
      builder.setPartition(this.partitionMethodDesc.getProto());
    }
    if (this.external != null) {
      builder.setIsExternal(external);
    }

    return builder.build();
  }
}