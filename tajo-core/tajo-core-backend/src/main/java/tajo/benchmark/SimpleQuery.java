package tajo.benchmark;

import java.io.IOException;

public class SimpleQuery extends TPCH {
  private final String BENCHMARK_DIR = "benchmark/simple";

  public void loadQueries() throws IOException {
    loadQueries(BENCHMARK_DIR);
  }
}
