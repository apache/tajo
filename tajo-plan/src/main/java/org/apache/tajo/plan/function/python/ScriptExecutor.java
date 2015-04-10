package org.apache.tajo.plan.function.python;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public interface ScriptExecutor {
//  void start(FunctionInvokeContext context) throws IOException;
  void start(OverridableConf queryContext) throws IOException;
  void shutdown() throws IOException;
  Datum eval(Tuple input);
}
