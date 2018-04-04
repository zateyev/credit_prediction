package kz.greetgo.credit_prediction.prepare_input_data.controller;

import kz.greetgo.credit_prediction.prepare_input_data.client_to_json.SelectorAsJson;
import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.parser.ContractsRespParser;
import kz.greetgo.credit_prediction.prepare_input_data.parser.OverduesRespParser;
import kz.greetgo.credit_prediction.prepare_input_data.parser.TransactionsRespParser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MigrationController implements AutoCloseable {

  Connection connection;
  int maxBatchSize;
  private String pathToRawFiles;

  public MigrationController(Connection connection, int maxBatchSize) {
    this.connection = connection;
    this.maxBatchSize = maxBatchSize;
  }

  public static void main(String[] args) throws Exception {
    Connection connection = DbAccess.createConnection();
    try (
      SelectorAsJson selectorAsJson = new SelectorAsJson(connection);
      MigrationController mc = new MigrationController(connection, 10_000)
    ) {

      mc.pathToRawFiles = "/home/zateyev/raw_data/";

      mc.migrateToTmp();
      mc.deleteDuplicateRecords();

      selectorAsJson.createClientJsonFiles("build/json_files/");
    }
  }

  public void migrateToTmp() throws Exception {
    List<String> contractFileDirs = getOverdueFiles(pathToRawFiles + "getContracts");
    List<String> overdueFileDirs = getOverdueFiles(pathToRawFiles + "getOverdues");
    List<String> transactionFileDirs = getOverdueFiles(pathToRawFiles + "getTransactions");

    try (
      ContractsRespParser contractsRespParser = new ContractsRespParser(connection, maxBatchSize);
      OverduesRespParser overduesRespParser = new OverduesRespParser(connection, maxBatchSize);
      TransactionsRespParser transactionsRespParser = new TransactionsRespParser(connection, maxBatchSize)
      ) {

      for (String contractFileDir : contractFileDirs) {
        Path path = Paths.get(contractFileDir);
        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        contractsRespParser.read(fileInputStream);
        System.out.println("File " + contractFileDir + " parsed");
      }

      for (String overdueFileDir : overdueFileDirs) {
        System.out.println("Reading file " + overdueFileDir);
        Path path = Paths.get(overdueFileDir);
        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        overduesRespParser.read(fileInputStream);
      }

      for (String transactionFileDir : transactionFileDirs) {
        Path path = Paths.get(transactionFileDir);
        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        transactionsRespParser.read(fileInputStream);
        System.out.println("File " + transactionFileDir + " parsed");
      }
    }
  }

  private List<TarArchiveEntry> getContractFiles(String tarDir) throws IOException {
    List<TarArchiveEntry> ret = new ArrayList<>();
    TarArchiveInputStream tarInput = new TarArchiveInputStream(new FileInputStream(tarDir));
    TarArchiveEntry entry;
    while (null != (entry = tarInput.getNextTarEntry())) {
      if (entry.getName().endsWith(".txt")) {

        ret.add(entry);
      }
    }
    return ret;
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

  private List<String> getOverdueFiles(String path) {
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
  }
}
