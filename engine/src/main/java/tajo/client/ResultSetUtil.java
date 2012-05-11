package tajo.client;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author Hyunsik Choi
 */
public class ResultSetUtil {
  public static String prettyFormat(ResultSet res) throws SQLException {
    StringBuilder sb = new StringBuilder();
    ResultSetMetaData rsmd = res.getMetaData();
    int numOfColumns = rsmd.getColumnCount();

    for (int i = 1; i <= numOfColumns; i++) {
      if (i > 1) sb.append(",  ");
      String columnName = rsmd.getColumnName(i);
      sb.append(columnName);
    }
    sb.append("\n-------------------------------\n");

    while (res.next()) {
      for (int i = 1; i <= numOfColumns; i++) {
        if (i > 1) sb.append(",  ");
        String columnValue = res.getObject(i).toString();
        sb.append(columnValue);
      }
      sb.append("\n");
    }

    return sb.toString();
  }
}
