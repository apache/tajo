package org.apache.tajo.plan.function;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public abstract class AggFunctionInvoke implements Cloneable {
  @Expose protected FunctionDesc functionDesc;

  public AggFunctionInvoke(FunctionDesc functionDesc) {
    this.functionDesc = functionDesc;
  }

  public void setFunctionDesc(FunctionDesc functionDesc) {
    this.functionDesc = functionDesc;
  }

  public abstract void init(FunctionInvokeContext context) throws IOException;

  public abstract FunctionContext newContext();

  public abstract void eval(FunctionContext context, Tuple params);

  public abstract void merge(FunctionContext context, Tuple params);

  public abstract Datum terminate(FunctionContext context);

  @Override
  public boolean equals(Object o) {
    if (o instanceof AggFunctionInvoke) {
      AggFunctionInvoke other = (AggFunctionInvoke) o;
      return this.functionDesc.equals(other.functionDesc);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return functionDesc.hashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    AggFunctionInvoke clone = (AggFunctionInvoke) super.clone();
    clone.functionDesc = (FunctionDesc) this.functionDesc.clone();
    return clone;
  }
}
