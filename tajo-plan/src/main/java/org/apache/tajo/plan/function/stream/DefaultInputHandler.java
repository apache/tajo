package org.apache.tajo.plan.function.stream;

import org.apache.tajo.catalog.Schema;

import java.io.IOException;

public class DefaultInputHandler extends InputHandler {

  public DefaultInputHandler(Schema schema) {
    serializer = RowStoreUtil.createEncoder(schema);
  }

  public DefaultInputHandler(HandleSpec spec) {
    serializer = (PigToStream)PigContext.instantiateFuncFromSpec(spec.spec);
  }

  @Override
  public InputType getInputType() {
    return InputType.SYNCHRONOUS;
  }

  @Override
  public synchronized void close(Process process) throws IOException {
    try {
      super.close(process);
    } catch(IOException e) {
      // check if we got an exception because
      // the process actually completed and we were
      // trying to flush and close it's stdin
      if (process == null || process.exitValue() != 0) {
        // the process had not terminated normally
        // throw the exception we got
        throw e;
      }
    }
  }
