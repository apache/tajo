package tajo.engine.planner.physical;

import org.apache.hadoop.fs.Path;
import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.planner.logical.SortNode;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.util.*;

/**
 * @author Byungnam Lim
 */
public class ExternalSortExec extends PhysicalExec {
  private SortNode annotation;
  private PhysicalExec subOp;
  private final Schema inputSchema;
  private final Schema outputSchema;

  private final Comparator<Tuple> comparator;
  private final List<Tuple> tupleSlots;
  private boolean sorted = false;
  private StorageManager sm;
  private tajo.storage.Scanner s;
  private Appender appender;
  private String tableName = null;


  private final String workDir;
  private int SORT_BUFFER_SIZE;
  private int run;
  private final static String SORT_PREFIX = "s_";

  public ExternalSortExec(TajoConf conf, SubqueryContext ctx, StorageManager sm, SortNode annotation,
      PhysicalExec subOp) {
    this.annotation = annotation;
    this.subOp = subOp;
    this.sm = sm;

    this.SORT_BUFFER_SIZE = conf.getIntVar(ConfVars.EXTERNAL_SORT_BUFFER);

    this.inputSchema = annotation.getInSchema();
    this.outputSchema = annotation.getOutSchema();

    this.comparator = new TupleComparator(inputSchema, annotation.getSortKeys());
    this.tupleSlots = new ArrayList<Tuple>(SORT_BUFFER_SIZE);

    this.run = 0;
    this.workDir = ctx.getWorkDir().getAbsolutePath() + "/" + UUID.randomUUID();
  }

  public PhysicalExec getSubOp(){
    return this.subOp;
  }

  public SortNode getAnnotation() {
    return this.annotation;
  }

  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }

  private void firstPhase(List<Tuple> tupleSlots) throws IOException {
    TableMeta meta = TCatUtil.newTableMeta(this.inputSchema, StoreType.RAW);
    Collections.sort(tupleSlots, this.comparator);
    Path localPath = new Path(workDir, SORT_PREFIX + "0_" + run);
    sm.initLocalTableBase(localPath, meta);
    appender = sm.getLocalAppender(meta, new Path(localPath, "data/" + SORT_PREFIX + "0_" + run));
    for (Tuple t : tupleSlots) {
      appender.addTuple(t);
    }
    appender.flush();
    appender.close();
    tupleSlots.clear();
    run++;
  }

  @Override
  public Tuple next() throws IOException {
    if (!sorted) {
      Tuple tuple;
      int runNum;
      while ((tuple = subOp.next()) != null) { // partition sort start
        tupleSlots.add(new VTuple(tuple));
        if (tupleSlots.size() == SORT_BUFFER_SIZE) {
          firstPhase(tupleSlots);
        }
      }

      if (tupleSlots.size() != 0) {
        firstPhase(tupleSlots);
      }

      if (run == 0) {
        // if there are no data
        return null;
      }

      runNum = run;

      int iterator = 0;
      run = 0;

      TableMeta meta;
      // external sort start
      while (runNum > 1) {
        while (run < runNum) {
          meta = TCatUtil.newTableMeta(this.inputSchema, StoreType.RAW);
          Path localPath = new Path(workDir, SORT_PREFIX + (iterator + 1) + "_" + (run / 2));
          sm.initLocalTableBase(localPath, meta);
          appender = sm.getLocalAppender(meta, new Path(localPath, "data/" + SORT_PREFIX + (iterator + 1) + "_" + (run / 2)));

          if (run + 1 >= runNum) { // if number of run is odd just copy it.
            Path p2 = new Path(workDir, SORT_PREFIX + iterator + "_" + run);
            tajo.storage.Scanner s1 = sm.getLocalScanner(p2, SORT_PREFIX + iterator + "_" + run);
            while ((tuple = s1.next()) != null) {
              appender.addTuple(tuple);
            }
          } else {
            Path p2 = new Path(workDir, SORT_PREFIX + iterator + "_" + run);
            tajo.storage.Scanner s1 = sm.getLocalScanner(p2, SORT_PREFIX + iterator + "_" + run);
            Path p3 = new Path(workDir, SORT_PREFIX + iterator + "_" + (run + 1));
            tajo.storage.Scanner s2 = sm.getLocalScanner(p3, SORT_PREFIX + iterator + "_" + (run + 1));

            Tuple left = s1.next();
            Tuple right = s2.next();

            while (left != null && right != null) {
              if (this.comparator.compare(left, right) < 0) {
                appender.addTuple(left);
                left = s1.next();
              } else {
                appender.addTuple(right);
                right = s2.next();
              }
            }

            if (left == null) {
              appender.addTuple(right);
              while ((right = s2.next()) != null) {
                appender.addTuple(right);
              }
            } else {
              appender.addTuple(left);
              while ((left = s1.next()) != null) {
                appender.addTuple(left);
              }
            }
          }
          appender.flush();
          appender.close();
          run += 2;
        }
        iterator++;
        run = 0;
        runNum = runNum / 2 + runNum % 2;
      }
      tableName = SORT_PREFIX + iterator + "_" + 0;
      s = sm.getLocalScanner(new Path(workDir, tableName), tableName);
      sorted = true;
    }

    return s.next();
  }

  @Override
  public void rescan() throws IOException {
    if (s != null) {
      s.reset();
    }
  }
}
