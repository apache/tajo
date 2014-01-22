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

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogProtocol.CatalogProtocolService;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.ServerCallable;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CatalogClient provides a client API to access the catalog server.
 */
public abstract class AbstractCatalogClient implements CatalogService {
  private final Log LOG = LogFactory.getLog(AbstractCatalogClient.class);

  protected RpcConnectionPool pool;
  protected InetSocketAddress catalogServerAddr;
  protected TajoConf conf;

  abstract CatalogProtocolService.BlockingInterface getStub(NettyClientBase client);

  public AbstractCatalogClient(TajoConf conf, InetSocketAddress catalogServerAddr) {
    this.pool = RpcConnectionPool.getPool(conf);
    this.catalogServerAddr = catalogServerAddr;
    this.conf= conf;
  }

  @Override
  public final TableDesc getTableDesc(final String name) {
    try {
      return new ServerCallable<TableDesc>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public TableDesc call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return CatalogUtil.newTableDesc(stub.getTableDesc(null, StringProto.newBuilder().setValue(name).build()));
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final Collection<String> getAllTableNames() {
    try {
      return new ServerCallable<Collection<String>>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Collection<String> call(NettyClientBase client) throws ServiceException {
          List<String> protos = new ArrayList<String>();
          GetAllTableNamesResponse response;
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          response = stub.getAllTableNames(null, NullProto.newBuilder().build());
          int size = response.getTableNameCount();
          for (int i = 0; i < size; i++) {
            protos.add(response.getTableName(i));
          }
          return protos;
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    try {
      return new ServerCallable<Collection<FunctionDesc>>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Collection<FunctionDesc> call(NettyClientBase client) throws ServiceException {
          List<FunctionDesc> list = new ArrayList<FunctionDesc>();
          GetFunctionsResponse response;
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          response = stub.getFunctions(null, NullProto.newBuilder().build());
          int size = response.getFunctionDescCount();
          for (int i = 0; i < size; i++) {
            try {
              list.add(new FunctionDesc(response.getFunctionDesc(i)));
            } catch (ClassNotFoundException e) {
              LOG.error(e);
              return null;
            }
          }
          return list;
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final boolean addTable(final TableDesc desc) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.addTable(null, desc.getProto()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean deleteTable(final String name) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.deleteTable(null,
              StringProto.newBuilder().setValue(name).build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean existsTable(final String tableId) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub
              .existsTable(null, StringProto.newBuilder().setValue(tableId).build())
              .getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean addIndex(final IndexDesc index) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.addIndex(null, index.getProto()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean existIndex(final String indexName) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existIndexByName(null, StringProto.newBuilder().
              setValue(indexName).build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean existIndex(final String tableName, final String columnName) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          GetIndexRequest.Builder builder = GetIndexRequest.newBuilder();
          builder.setTableName(tableName);
          builder.setColumnName(columnName);
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.existIndex(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final IndexDesc getIndex(final String indexName) {
    try {
      return new ServerCallable<IndexDesc>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public IndexDesc call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return new IndexDesc(
              stub.getIndexByName(null,
                  StringProto.newBuilder().setValue(indexName).build()));
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public final IndexDesc getIndex(final String tableName, final String columnName) {
    try {
      return new ServerCallable<IndexDesc>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public IndexDesc call(NettyClientBase client) throws ServiceException {
          GetIndexRequest.Builder builder = GetIndexRequest.newBuilder();
          builder.setTableName(tableName);
          builder.setColumnName(columnName);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return new IndexDesc(stub.getIndex(null, builder.build()));
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public boolean deleteIndex(final String indexName) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.delIndex(null,
              StringProto.newBuilder().setValue(indexName).build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean createFunction(final FunctionDesc funcDesc) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.createFunction(null, funcDesc.getProto()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final boolean dropFunction(final String signature) {
    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          UnregisterFunctionRequest.Builder builder = UnregisterFunctionRequest.newBuilder();
          builder.setSignature(signature);

          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.dropFunction(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public final FunctionDesc getFunction(final String signature, DataType... paramTypes) {
    return getFunction(signature, null, paramTypes);
  }

  @Override
  public final FunctionDesc getFunction(final String signature, FunctionType funcType, DataType... paramTypes) {
    final GetFunctionMetaRequest.Builder builder = GetFunctionMetaRequest.newBuilder();
    builder.setSignature(signature);
    if (funcType != null) {
      builder.setFunctionType(funcType);
    }
    for (DataType type : paramTypes) {
      builder.addParameterTypes(type);
    }

    FunctionDescProto descProto = null;
    try {
      descProto = new ServerCallable<FunctionDescProto>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public FunctionDescProto call(NettyClientBase client) throws ServiceException {
          try {
            CatalogProtocolService.BlockingInterface stub = getStub(client);
            return stub.getFunctionMeta(null, builder.build());
          } catch (NoSuchFunctionException e) {
            abort();
            throw e;
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
    }

    if (descProto == null) {
      throw new NoSuchFunctionException(signature);
    }
    if(descProto == null) {
      LOG.error("No matched function:" + signature + "," + funcType + "," + paramTypes);
      return null;
    }
    try {
      return new FunctionDesc(descProto);
    } catch (ClassNotFoundException e) {
      LOG.error(e);
      throw new NoSuchFunctionException(signature);
    }
  }

  @Override
  public final boolean containFunction(final String signature, DataType... paramTypes) {
    return containFunction(signature, null, paramTypes);
  }

  @Override
  public final boolean containFunction(final String signature, FunctionType funcType, DataType... paramTypes) {
    final ContainFunctionRequest.Builder builder =
        ContainFunctionRequest.newBuilder();
    if (funcType != null) {
      builder.setFunctionType(funcType);
    }
    builder.setSignature(signature);
    for (DataType type : paramTypes) {
      builder.addParameterTypes(type);
    }

    try {
      return new ServerCallable<Boolean>(conf, catalogServerAddr, CatalogProtocol.class, false) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          CatalogProtocolService.BlockingInterface stub = getStub(client);
          return stub.containFunction(null, builder.build()).getValue();
        }
      }.withRetries();
    } catch (ServiceException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
}