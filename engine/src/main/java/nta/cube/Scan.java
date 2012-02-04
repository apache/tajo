package nta.cube;

import java.io.IOException;
import java.util.LinkedList;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.storage.Scanner;
import nta.storage.StorageManager;

import org.apache.hadoop.conf.Configuration;

public class Scan {
  public static LinkedList<Row> localscan(CubeConf conf) throws IOException {
    Schema schema = conf.getInschema();

    LinkedList<Row> rowlist = new LinkedList<Row>();
    StorageManager sm = StorageManager.get(new Configuration(), Cons.datapath);

    Scanner scanner = sm.getTableScanner(conf.getLocalInput());
    nta.storage.Tuple t = null;
    while ((t = scanner.next()) != null) {
      Row row;

      if (schema.getColumn("count") == null) {
        row = new Row(schema.getColumnNum());
        for (int w = 0; w < schema.getColumnNum(); w++) {
          row.values[w] = t.get(w);
        }
        row.count = 1;
      } else {
        row = new Row(conf.getInschema());
        for (int w = 0; w < schema.getColumnNum() - 1; w++) {
          row.values[w] = t.get(w);
        }
        row.count = t.get(schema.getColumnId("count") - 1).asInt();
      }

      rowlist.add(row);
    }
    return rowlist;
  }

  public static LinkedList<Row> globalscan(CubeConf conf) throws IOException {
    conf.getInschema().addColumn("count", DataType.INT);
    LinkedList<Row> rowlist = new LinkedList<Row>();
    Schema schema = conf.getOutschema();
    schema.addColumn("count", DataType.INT);

    StorageManager sm = StorageManager.get(new Configuration(), Cons.datapath);

    Scanner scanner = sm.getTableScanner(Cons.immediatepath);

    nta.storage.Tuple t = null;

    while ((t = scanner.next()) != null) {
      Row row = new Row(conf.getOutschema());
      for (int w = 0; w < schema.getColumnNum() - 1; w++) {
        row.values[w] = t.get(w);
      }
      row.count = t.get(schema.getColumnId("count")).asInt();
      rowlist.add(row);
    }
    scanner.close();

    return rowlist;
  }
}
