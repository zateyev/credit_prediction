package kz.greetgo.credit_prediction.prepare_input_data.controller;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.parser.ContractsRespParser;
import kz.greetgo.credit_prediction.prepare_input_data.parser.OverduesRespParser;
import kz.greetgo.credit_prediction.prepare_input_data.parser.TransactionsRespParser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class MigrationController {
  public void migrate() throws Exception {
    String homePath = "/home/zateyev/raw_data/";
    List<String> contractFileDirs = getOverdueFiles(homePath + "getContracts");
    List<String> overdueFileDirs = getOverdueFiles(homePath + "getOverdues");
    List<String> transactionFileDirs = getOverdueFiles(homePath + "getTransactions");

    Connection connection = DbAccess.createConnection();
    int maxBatchSize = 10_000;
    ContractsRespParser contractsRespParser = new ContractsRespParser(connection, maxBatchSize);
    OverduesRespParser overduesRespParser = new OverduesRespParser(connection, maxBatchSize);
    TransactionsRespParser transactionsRespParser = new TransactionsRespParser(connection, maxBatchSize);

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

  public static void main(String[] args) throws Exception {
    MigrationController mc = new MigrationController();
    mc.migrate();
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

  private List<String> getTransactionFiles() {
    return null;
  }
}
