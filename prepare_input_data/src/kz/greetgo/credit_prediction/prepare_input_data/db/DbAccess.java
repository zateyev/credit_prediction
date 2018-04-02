package kz.greetgo.credit_prediction.prepare_input_data.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DbAccess {
  public static Connection createConnection() throws Exception {
    Class.forName("org.postgresql.Driver");

    return DriverManager.getConnection("jdbc:postgresql://localhost/asd", "asd", "asd");
  }

  private static final Map<String, String> alreadyRunSqlMap = new HashMap<>();

  public static void createTable(Connection connection, String sql) throws SQLException {
    if (alreadyRunSqlMap.containsKey(sql)) return;
    runSql(connection, sql);
    alreadyRunSqlMap.put(sql, sql);
  }

  private static void runSql(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      if ("42P07".equals(e.getSQLState())) return;
      throw e;
    }
  }
}
