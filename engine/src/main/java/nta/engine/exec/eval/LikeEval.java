/**
 * 
 */
package nta.engine.exec.eval;

import java.util.regex.Pattern;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.BoolDatum;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.StringDatum;
import nta.storage.Tuple;

import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class LikeEval extends EvalNode {
  @Expose private boolean not;
  @Expose private Column column;
  @Expose private String pattern;
  private int fieldId = -1;
  private Pattern compiled;
  private BoolDatum result;
  
  public LikeEval() {
    super(Type.LIKE);
  }
  
  public LikeEval(boolean not, Column column, String pattern) {
    this();
    this.not = not;
    this.column = column;
    this.setPattern(pattern);
  }
  
  public void setPattern(String pattern) {
    this.pattern = pattern;
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
  
  public String getPattern() {
    return this.pattern;
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple, Datum... args) {
    if (fieldId == -1) {
      fieldId = schema.getColumnId(column.getQualifiedName());
    }    
    StringDatum str = tuple.getString(fieldId);
    if (not) {
      result.setValue(!compiled.matcher(str.asChars()).matches());      
    } else {
      result.setValue(compiled.matcher(str.asChars()).matches());
    }
    
    return result;
  }
}