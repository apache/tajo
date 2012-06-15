/**
 * 
 */
package nta.engine.exec.eval;

import com.google.gson.annotations.Expose;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.BoolDatum;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.StringDatum;
import nta.storage.Tuple;

import java.util.regex.Pattern;

/**
 * @author Hyunsik Choi
 */
public class LikeEval extends BinaryEval {
  @Expose private boolean not;
  @Expose private Column column;
  @Expose private String pattern;

  // temporal variables
  private Integer fieldId = null;
  private Pattern compiled;
  private BoolDatum result;

  
  public LikeEval(boolean not, FieldEval field, ConstEval pattern) {
    super(Type.LIKE, field, pattern);
    this.not = not;
    this.column = field.getColumnRef();
    this.pattern = pattern.getValue().asChars();
  }
  
  public void compile(String pattern) {
    String regex = pattern.replace("?", ".");
    regex = regex.replace("%", ".*");
    
    this.compiled = Pattern.compile(regex,
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    result = DatumFactory.createBool(false);
  }

  @Override
  public DataType getValueType() {
    return DataType.BOOLEAN;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    if (fieldId == null) {
      fieldId = schema.getColumnId(column.getQualifiedName());
      compile(this.pattern);
    }    
    StringDatum str = tuple.getString(fieldId);
    if (not) {
      result.setValue(!compiled.matcher(str.asChars()).matches());      
    } else {
      result.setValue(compiled.matcher(str.asChars()).matches());
    }
  }

  public Datum terminate(EvalContext ctx) {
    return result;
  }
  
  @Override
  public String toString() {
    return this.column + " like '" + pattern +"'";
  }
}