package tajo.engine.function.builtin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.datum.LongDatum;
import tajo.engine.function.GeneralFunction;
import tajo.storage.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author jihoon
 */
public class Date extends GeneralFunction<LongDatum> {
  private final Log LOG = LogFactory.getLog(Date.class);
  private final static String dateFormat = "dd/MM/yyyy HH:mm:ss";

  public Date() {
    super(new Column[] {new Column("val", DataType.STRING)});
  }

  @Override
  public Datum eval(Tuple params) {
    try {
      return DatumFactory.createLong(new SimpleDateFormat(dateFormat)
          .parse(params.get(0).asChars()).getTime());
    } catch (ParseException e) {
      LOG.error(e);
      return null;
    }
  }
}
