package kz.greetgo.credit_prediction.prepare_input_data.migration;

import kz.greetgo.credit_prediction.prepare_input_data.parser.ContractsRespParser;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MigrationWorker {

  public Connection connection;
  public InputStream inputStream;

  public void migrate() throws Exception {
    migrateToTmp();
    migrateFromTmp();
  }

  private void migrateToTmp() throws Exception {
    ContractsRespParser contractsRespParser = new ContractsRespParser(connection, 10_000);
    contractsRespParser.read(inputStream);
  }

  private void migrateFromTmp() throws SQLException {
    validateErrors();
    deleteDuplicateRecords();
    checkForExistingRecords();
    upsertRecords();
  }

  private void validateErrors() {

  }

  public void deleteDuplicateRecords() throws SQLException {
    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT no, row_number() OVER(PARTITION BY clientid ORDER BY no DESC) AS ord \n" +
      "  FROM client_tmp WHERE status = 0\n" +
      ")\n" +
      "\n" +
      "UPDATE client_tmp tc SET status = 2 FROM num_ord\n" +
      "WHERE tc.no = num_ord.no AND num_ord.ord > 1");
    //language=PostgreSQL
    exec("DELETE FROM client_tmp WHERE status = 2");

    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT no, row_number() OVER(PARTITION BY contractid ORDER BY no DESC) AS ord \n" +
      "  FROM credit_tmp WHERE status = 0\n" +
      ")\n" +
      "\n" +
      "UPDATE credit_tmp tc SET status = 2 FROM num_ord\n" +
      "WHERE tc.no = num_ord.no AND num_ord.ord > 1");
    //language=PostgreSQL
    exec("DELETE FROM credit_tmp WHERE status = 2");

    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT no, row_number() OVER(PARTITION BY phoneid ORDER BY no DESC) AS ord \n" +
      "  FROM phone_tmp WHERE status = 0\n" +
      ")\n" +
      "\n" +
      "UPDATE phone_tmp tc SET status = 2 FROM num_ord\n" +
      "WHERE tc.no = num_ord.no AND num_ord.ord > 1");
    //language=PostgreSQL
    exec("DELETE FROM phone_tmp WHERE status = 2");
  }

  private void exec(String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
    }
  }

  private void checkForExistingRecords() {

  }

  private void upsertRecords() {

  }

}
