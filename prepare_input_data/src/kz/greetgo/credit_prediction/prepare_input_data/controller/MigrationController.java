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

//      mc.pathToRawFiles = "/home/zateyev/credit_prediction/raw_data";
      mc.pathToRawFiles = "/home/zateyev/Gshare/credit_prediction/raw_data";

      mc.migrateToTmp();
      mc.deleteDuplicateRecords();

//      selectorAsJson.createClientJsonFiles("/home/zateyev/credit_prediction/structured_json_files");
      selectorAsJson.createClientJsonFiles("build/json_files");
    }
  }

  public void migrateToTmp() throws Exception {
    List<String> contractFileDirs = getLogFileDirs(pathToRawFiles + "/getContracts");
    List<String> overdueFileDirs = getLogFileDirs(pathToRawFiles + "/getOverdues");
    List<String> transactionFileDirs = getLogFileDirs(pathToRawFiles + "/getTransactions");

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

  private void uploadFromFiles(ParserAbstract parser, List<String> contractFileDirs) throws Exception {
    for (String contractFileDir : contractFileDirs) {
      Path path = Paths.get(contractFileDir);
      FileInputStream fileInputStream = new FileInputStream(path.toFile());
      parser.read(fileInputStream);

      if (showStatus.get()) {
        showStatus.set(false);
        System.out.println("[Uploaded]: " + contractFileDir);
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
    List<String> ret = new ArrayList<>();

    File folder = new File(path);
    List<String> folders = new ArrayList<>();
    //noinspection ConstantConditions
    for (File folder1 : folder.listFiles()) {
      String fileName = folder1.getName();
      if (!fileName.endsWith(".tar")) {
        folders.add(fileName);
      }
    }
    folders.sort(String::compareTo);
    for (String s : folders) {
      if (!s.endsWith(".tar")) {
        folder = new File(path + "/" + s);
        List<String> fileDirs = new ArrayList<>();
        //noinspection ConstantConditions
        for (File file : folder.listFiles()) {
          String name = file.getName();
          fileDirs.add(path + "/" + s + "/" + name);
        }
        fileDirs.sort(String::compareTo);
        ret.addAll(fileDirs);
      }
    }

    return ret;
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
