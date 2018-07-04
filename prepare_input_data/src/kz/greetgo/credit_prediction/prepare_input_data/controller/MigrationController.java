package kz.greetgo.credit_prediction.prepare_input_data.controller;

import kz.greetgo.credit_prediction.prepare_input_data.client_to_json.SelectorAsJson;
import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.parser.ContractsRespParser;
import kz.greetgo.credit_prediction.prepare_input_data.parser.OverduesRespParser;
import kz.greetgo.credit_prediction.prepare_input_data.parser.ParserAbstract;
import kz.greetgo.credit_prediction.prepare_input_data.parser.TransactionsRespParser;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MigrationController implements AutoCloseable {

  Connection connection;
  int maxBatchSize;
  private String pathToRawFiles;

  private final AtomicBoolean working;
  private final AtomicBoolean showStatus;

  public MigrationController(Connection connection, int maxBatchSize) {
    this.connection = connection;
    this.maxBatchSize = maxBatchSize;

    working = new AtomicBoolean(true);
    showStatus = new AtomicBoolean(false);
    final Thread see = new Thread(() -> {

      while (working.get()) {

        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          break;
        }

        showStatus.set(true);

      }

    });
    see.start();
  }

  public static void main(String[] args) throws Exception {
    Connection connection = DbAccess.createConnection();
    try (
      SelectorAsJson selectorAsJson = new SelectorAsJson(connection);
      MigrationController mc = new MigrationController(connection, 10_000)
    ) {

      mc.pathToRawFiles = "/home/zateyev/credit_prediction/raw_data";
      mc.pathToRawFiles = "/home/zateyev/credit_prediction/humo";

      mc.migrateToTmp();
      mc.deleteDuplicateRecords();

      mc.createViews();

      selectorAsJson.createClientJsonFiles("/home/zateyev/credit_prediction/humo_json");
//      selectorAsJson.createClientJsonFiles("build/json_files");
    }
  }

  private void createViews() throws SQLException {
    DbAccess.createTable(connection, "create view v_overdue as select array_to_json(array_agg(overdue))::text as json," +
      " contract_id FROM overdue group by contract_id");

    DbAccess.createTable(connection, "create view v_fact_oper as select array_to_json(array_agg(fact_oper))::text as json," +
      " contract_id FROM fact_oper group by contract_id");

    DbAccess.createTable(connection, "create view v_acc_move as select array_to_json(array_agg(acc_move))::text as json," +
      " contract_id FROM acc_move group by contract_id");

    DbAccess.createTable(connection, "create view v_collateral as select array_to_json(array_agg(collateral))::text as json," +
      " contract_id FROM collateral group by contract_id");
  }

  public void migrateToTmp() throws Exception {
    List<String> contractFileDirs = getLogFileFolders(pathToRawFiles + "/getContracts");
    List<String> overdueFileDirs = getLogFileFolders(pathToRawFiles + "/getOverdues");
    List<String> transactionFileDirs = getLogFileFolders(pathToRawFiles + "/getTransactions");

    try (
      ParserAbstract contractsRespParser = new ContractsRespParser(connection, maxBatchSize);
      ParserAbstract overduesRespParser = new OverduesRespParser(connection, maxBatchSize);
      ParserAbstract transactionsRespParser = new TransactionsRespParser(connection, maxBatchSize)
    ) {

      uploadFromFiles(contractsRespParser, contractFileDirs);
      uploadFromFiles(overduesRespParser, overdueFileDirs);
      uploadFromFiles(transactionsRespParser, transactionFileDirs);

    }
  }

  private void uploadFromFiles(ParserAbstract parser, List<String> logFileFolders) throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    for (String folder : logFileFolders) {
      String[] split = folder.split("/");
      parser.downloadDate = sdf.parse(split[split.length - 1]);
      List<String> fileDirs = getLogFileDirs(folder);
      for (String fileName : fileDirs) {
        Path path = Paths.get(fileName);
        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        parser.read(fileInputStream);

        if (showStatus.get()) {
          showStatus.set(false);
          System.out.println("[Uploaded]: " + folder);
        }
      }
    }
  }

  private void deleteDuplicateRecords() throws SQLException {
    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT no, row_number() OVER(PARTITION BY client_id ORDER BY no DESC) AS ord \n" +
      "  FROM client_tmp WHERE status = 0\n" +
      ")\n" +
      "\n" +
      "UPDATE client_tmp tc SET status = 2 FROM num_ord\n" +
      "WHERE tc.no = num_ord.no AND num_ord.ord > 1");
    //language=PostgreSQL
    exec("DELETE FROM client_tmp WHERE status = 2");

    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT no, row_number() OVER(PARTITION BY contract_id ORDER BY no DESC) AS ord \n" +
      "  FROM credit_tmp WHERE status = 0\n" +
      ")\n" +
      "\n" +
      "UPDATE credit_tmp tc SET status = 2 FROM num_ord\n" +
      "WHERE tc.no = num_ord.no AND num_ord.ord > 1");
    //language=PostgreSQL
    exec("DELETE FROM credit_tmp WHERE status = 2");

    //language=PostgreSQL
    exec("WITH num_ord AS (\n" +
      "  SELECT no, row_number() OVER(PARTITION BY phone_id ORDER BY no DESC) AS ord \n" +
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

  private List<String> getLogFileDirs(String path) {
    File folder = new File(path);
    List<String> fileDirs = new ArrayList<>();
    //noinspection ConstantConditions
    for (File file : folder.listFiles()) {
      String name = file.getName();
      fileDirs.add(path + "/" + name);
    }
    fileDirs.sort(String::compareTo);

    return fileDirs;
  }

  private List<String> getLogFileFolders(String path) {
    File folder = new File(path);
    List<String> folders = new ArrayList<>();
    //noinspection ConstantConditions
    for (File folder1 : folder.listFiles()) {
      String folder1Name = folder1.getName();
      if (!folder1Name.endsWith(".tar")) {
        folders.add(path + "/" + folder1Name);
      }
    }
    folders.sort(String::compareTo);
    return folders;
  }

  @Override
  public void close() throws Exception {
    if (connection != null) {
      connection.close();
      connection = null;
    }
    working.set(false);
  }
}
