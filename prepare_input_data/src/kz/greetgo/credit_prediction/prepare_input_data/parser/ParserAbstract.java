package kz.greetgo.credit_prediction.prepare_input_data.parser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public abstract class ParserAbstract implements AutoCloseable {

  protected final int maxBatchSize;
  protected final Connection connection;

  protected int year, month, day;
  protected final List<CloseBracket> closeBracketList = new ArrayList<>();

  public ParserAbstract(Connection connection, int maxBatchSize) throws SQLException {
    this.maxBatchSize = maxBatchSize;
    this.connection = connection;
    createTables();
  }

  protected abstract void createTables() throws SQLException;

  public void read(Path path) throws Exception {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile()), "UTF-8"))) {
      int lineNo = 1;
      while (true) {
        String line = br.readLine();
        if (line == null) break;
        readLine(line, lineNo++);
      }
      finish();

    }
  }

  protected abstract void readLine(String line, int lineNo) throws SQLException;
  protected abstract void finish() throws SQLException;
  protected abstract void readKeyValue(String key, String value);

  protected Date readDate() {
    GregorianCalendar cal = new GregorianCalendar();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    return cal.getTime();
  }

  protected static java.sql.Date toDate(Date javaDate) {
    return javaDate == null ? null : new java.sql.Date(javaDate.getTime());
  }
}
