package tajo.benchmark;

import tajo.conf.TajoConf;

public class Driver {
  private static void printUsage() {
    System.out.println("benchmark BenchmarkClass datadir query");
  }

  public static void main(String [] args) throws Exception {

    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }

    TajoConf conf = new TajoConf();
    Class clazz = Class.forName(args[0]);
    BenchmarkSet benchmark = (BenchmarkSet) clazz.newInstance();

    benchmark.init(conf, args[1]);
    benchmark.loadSchemas();
    benchmark.loadQueries();
    benchmark.loadTables();
    benchmark.perform(args[2]);
    System.exit(0);
  }
}
