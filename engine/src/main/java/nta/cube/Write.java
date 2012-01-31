package nta.cube;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.engine.planner.logical.GroupbyNode;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.VTuple;

import org.apache.hadoop.conf.Configuration;

/* hdfs에 cuboid를 write */
public class Write {

  public void write(CubeConf conf, Cuboid cuboidin, String tableName)
      throws IOException {
    GroupbyNode gnode = Cons.gnode;
    Schema schema = conf.getOutschema();

    if (!conf.getOutschema().contains("count")) {
      schema.addColumn("count", DataType.INT);
    }

    StorageManager sm = StorageManager.get(new Configuration(), Cons.datapath);

    TableMeta meta = new TableMetaImpl();
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);
    File folder = new File(sm.getTablePath(tableName).toString());
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    if (!folder.exists()) {
      lock.writeLock().lock();
      if (!folder.exists()) {
        sm.initTableBase(meta, tableName);
        // if (folder.exists()) {
        // System.out.println("exist " + conf.getNodenum());
        // } else {
        // System.out.println("not exist " + conf.getNodenum());
        // }
      }
      lock.writeLock().unlock();
    }

    Appender appender = sm.getAppender(meta, tableName,
        "node" + conf.getNodenum());
    VTuple vTuple = null;
    for (SummaryTable st : cuboidin.cuboid) {
      for (KVpair kvpair : st.summary_table) {
        vTuple = new VTuple(schema.getColumnNum());
        for (int z = 0; z < gnode.getTargetList().length; z++) {
          if (z < kvpair.key.length) {
            vTuple.put(
                gnode
                    .getOutputSchema()
                    .getColumn(
                        gnode.getTargetList()[z].getColumnSchema().getName())
                    .getId(), kvpair.key[z]);
          } else {
            vTuple.put(
                gnode
                    .getOutputSchema()
                    .getColumn(
                        gnode.getTargetList()[z].getColumnSchema().getName())
                    .getId(), kvpair.val[z - kvpair.key.length]);
          }
        }

        for (int z = 0; z < kvpair.key.length; z++) {
          vTuple.put(z, kvpair.key[z]);
        }
        for (int z = 0; z < kvpair.val.length; z++) {
          vTuple.put(z + kvpair.key.length, kvpair.val[z]);
        }
        vTuple.put(kvpair.key.length + kvpair.val.length,
            DatumFactory.createInt(kvpair.count));
        appender.addTuple(vTuple);
      }
    }
    appender.close();
  }
}
