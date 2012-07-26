package tajo.engine;

import nta.catalog.Options;
import nta.catalog.Schema;
import nta.engine.NtaTestingUtility;
import nta.storage.CSVFile2;
import nta.util.FileUtil;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;
import tajo.benchmark.TPCH;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public class TpchTestBase {
  String [] names;
  String [] paths;
  String [][] tables;
  Schema[] schemas;
  Map<String, Integer> nameMap = Maps.newHashMap();
  protected TPCH tpch;

  public TpchTestBase() throws IOException {
    names = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
    paths = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      nameMap.put(names[i], i);
    }

    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadQueries();

    schemas = new Schema[names.length];
    for (int i = 0; i < names.length; i++) {
      schemas[i] = tpch.getSchema(names[i]);
    }

    tables = new String[names.length][];
    File file;
    for (int i = 0; i < names.length; i++) {
      file = new File("src/test/tpch/" + names[i] + ".tbl");
      tables[i] = FileUtil.readTextFile(file).split("\n");
      paths[i] = file.getAbsolutePath();
    }
  }

  public int getIndexByName(String name) {
    return nameMap.get(name);
  }

  public ResultSet execute(String query) throws Exception {
    Options opt = new Options();
    opt.put(CSVFile2.DELIMITER, "|");
//    return NtaTestingUtility.runInLocal(names, schemas, opt, tables, query);
    return NtaTestingUtility.run(names, paths, schemas, opt, query);
  }
}
