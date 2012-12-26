/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package tajo.catalog;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.CatalogProtocol.CatalogProtocolService;
import tajo.catalog.proto.CatalogProtos.*;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * CatalogClient provides a client API to access the catalog server.
 *
 * @author Hyunsik Choi
 *
 */
public abstract class AbstractCatalogClient implements CatalogService {
  private final Log LOG = LogFactory.getLog(AbstractCatalogClient.class);
  protected CatalogProtocolService.BlockingInterface stub;

  protected void setStub(CatalogProtocolService.BlockingInterface stub) {
    this.stub = stub;
  }

  protected CatalogProtocolService.BlockingInterface getStub() {
    return this.stub;
  }

  @Override
  public final TableDesc getTableDesc(final String name) {
    try {
      return TCatUtil.newTableDesc(stub.getTableDesc(null, StringProto.newBuilder()
          .setValue(name).build()));
    } catch (ServiceException e) {
      LOG.error(e);
      return null;
    }
  }

  @Override
  public final Collection<String> getAllTableNames() {
    List<String> protos = new ArrayList<String>();
    GetAllTableNamesResponse response;

    try {
      response = stub.getAllTableNames(null, NullProto.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error(e);
      return null;
    }
    int size = response.getTableNameCount();
    for (int i = 0; i < size; i++) {
      protos.add(response.getTableName(i));
    }
    return protos;
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();
    GetFunctionsResponse response;
    try {
      response = stub.getFunctions(null, NullProto.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error(e);
      return null;
    }
    int size = response.getFunctionDescCount();
    for (int i = 0; i < size; i++) {
      list.add(new FunctionDesc(response.getFunctionDesc(i)));
    }
    return list;
  }

  @Override
  public final boolean addTable(final TableDesc desc) {
    try {
      return stub.addTable(null, (TableDescProto) desc.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final boolean deleteTable(final String name) {
    try {
      return stub.deleteTable(null,
          StringProto.newBuilder().setValue(name).build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final boolean existsTable(final String tableId) {
    try {
      return stub
          .existsTable(null, StringProto.newBuilder().setValue(tableId).build())
          .getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final boolean addIndex(IndexDesc index) {
    try {
      return stub.addIndex(null, index.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final boolean existIndex(String indexName) {
    try {
      return stub.existIndexByName(null, StringProto.newBuilder().
          setValue(indexName).build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public boolean existIndex(String tableName, String columnName) {
    GetIndexRequest.Builder builder = GetIndexRequest.newBuilder();
    builder.setTableName(tableName);
    builder.setColumnName(columnName);
    try {
      return stub.existIndex(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final IndexDesc getIndex(String indexName) {
    try {
      return new IndexDesc(
          stub.getIndexByName(null,
              StringProto.newBuilder().setValue(indexName).build()));
    } catch (ServiceException e) {
      LOG.error(e);
      return null;
    }
  }

  @Override
  public final IndexDesc getIndex(String tableName, String columnName) {
    GetIndexRequest.Builder builder = GetIndexRequest.newBuilder();
    builder.setTableName(tableName);
    builder.setColumnName(columnName);
    try {
      return new IndexDesc(stub.getIndex(null, builder.build()));
    } catch (ServiceException e) {
      LOG.error(e);
      return null;
    }
  }

  @Override
  public boolean deleteIndex(String indexName) {
    try {
      return stub.delIndex(null,
          StringProto.newBuilder().setValue(indexName).build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final boolean registerFunction(final FunctionDesc funcDesc) {
    try {
      return stub.registerFunction(null, funcDesc.getProto()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final boolean unregisterFunction(final String signature,
                                          DataType... paramTypes) {
    UnregisterFunctionRequest.Builder builder =
        UnregisterFunctionRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    try {
      return stub.unregisterFunction(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }

  @Override
  public final FunctionDesc getFunction(final String signature,
                                        DataType... paramTypes) {
    GetFunctionMetaRequest.Builder builder =
        GetFunctionMetaRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }

    FunctionDescProto descProto;
    try {
      descProto = stub.getFunctionMeta(null, builder.build());
    } catch (ServiceException e) {
      LOG.error(e);
      return null;
    }
    return new FunctionDesc(descProto);
  }

  @Override
  public final boolean containFunction(final String signature,
                                       DataType... paramTypes) {
    ContainFunctionRequest.Builder builder =
        ContainFunctionRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    try {
      return stub.containFunction(null, builder.build()).getValue();
    } catch (ServiceException e) {
      LOG.error(e);
      return false;
    }
  }
}