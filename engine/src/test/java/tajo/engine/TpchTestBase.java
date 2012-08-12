package tajo.engine;

import com.google.common.collect.Maps;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.storage.CSVFile2;
import nta.util.FileUtil;
import tajo.TajoTestingUtility;
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
  protected TajoTestingUtility util;

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
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void setUp() throws Exception {
    util = new TajoTestingUtility();
    Options opt = new Options();
    opt.put(CSVFile2.DELIMITER, "|");
    util.setup(names, paths, schemas, opt);
  }

  public ResultSet execute(String query) throws Exception {
    return util.execute(query);
  }

  public void tearDown() throws IOException {
    util.shutdown();
  }
}
