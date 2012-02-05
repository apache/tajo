/**
 * 
 */
package nta.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.TableDescProto;

import org.apache.hadoop.conf.Configuration;

/**
 * This class provides a catalog service interface in
 * local.
 * 
 * @author Hyunsik Choi
 *
 */
public class LocalCatalog implements CatalogService {
  private CatalogServer catalog;
  
  public LocalCatalog(final Configuration conf) throws IOException {
    this.catalog = new CatalogServer(conf);
    this.catalog.start();
  }
  
  public LocalCatalog(final CatalogServer server) {
    this.catalog = server;
  }

  @Override
  public final TableDesc getTableDesc(final String name) {
    return TableDesc.Factory.create(catalog.getTableDesc(name));
  }

  @Override
  public final Collection<TableDesc> getAllTableDescs() {
    List<TableDesc> list = new ArrayList<TableDesc>();
    Collection<TableDescProto> protos
      = catalog.getAllTableDescs();
    for (TableDescProto proto : protos) {
      list.add(TableDesc.Factory.create(proto));
    }
    return list;
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();
    Collection<FunctionDescProto> protos
      = catalog.getFunctions();
    for (FunctionDescProto proto : protos) {
      list.add(new FunctionDesc(proto));
    }
    return list;
  }

  @Override
  public final void addTable(final TableDesc desc) {
    catalog.addTable((TableDescProto) desc.getProto());
  }

  @Override
  public final void deleteTable(final String name) {
    catalog.deleteTable(name);
  }

  @Override
  public final boolean existsTable(final String tableId) {
    return catalog.existsTable(tableId);
  }

  @Override
  public final void registerFunction(final FunctionDesc funcDesc) {
    catalog.registerFunction(funcDesc.getProto());
  }

  @Override
  public final void unregisterFunction(final String signature, 
      DataType...paramTypes) {
    catalog.unregisterFunction(signature, paramTypes);
  }

  @Override
  public final FunctionDesc getFunction(final String signature,
      DataType...paramTypes) {
    return FunctionDesc.create(catalog.getFunctionMeta(signature, paramTypes));
  }

  @Override
  public final boolean containFunction(final String signature, 
      DataType...paramTypes) {
    return catalog.containFunction(signature, paramTypes);
  }

  @Override
  public final List<TabletServInfo> getHostByTable(final String tableId) {    
    return catalog.getHostByTable(tableId);
  }
  
  @Override
  public final void updateAllTabletServingInfo(final List<String> onlineServers)
      throws IOException {
    catalog.updateAllTabletServingInfo(onlineServers);
  }
}
