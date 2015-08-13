package org.apache.tajo.engine.function.json;

import net.minidev.json.parser.JSONParser;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;

public abstract class ScalarJsonFunction extends GeneralFunction {
  protected JSONParser parser;

  public ScalarJsonFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public void init(OverridableConf queryContext, FunctionEval.ParamType [] paramTypes) {
    parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);
  }
}
