package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.contract.Client;
import kz.greetgo.credit_prediction.prepare_input_data.model.contract.ContractsResp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class ContractsRespParser implements AutoCloseable {

  PreparedStatement clientPS;
  private final int maxBatchSize;
  private final Connection connection;

  public ContractsRespParser(Connection connection, int maxBatchSize) throws SQLException {
    this.connection = connection;

    connection.setAutoCommit(true);

    this.maxBatchSize = maxBatchSize;
    DbAccess.createTable(connection, "create table contract_rest (" +
      "  asd varchar(100)" +
      ")");
    DbAccess.createTable(connection, "create table client (" +
      "  no         bigint primary key," +
      "  clientId   varchar(20)," +
      "  dateBirth  date," +
      "  firstname  varchar(300)," +
      "  surname    varchar(300)," +
      "  patronymic varchar(300)" +
      ")");

    connection.setAutoCommit(false);

    clientPS = connection.prepareStatement("insert into client (" +
      "no, clientId, dateBirth, firstname, surname, patronymic" +
      ") values (" +
      " ?,?,?,?,?,?" +
      ")");
  }

  int clientBatchSize = 0;

  private void goContractsResp() throws SQLException {
    if (contractsResp == null) return;

    clientPS.setLong(1, clientNo++);
    clientPS.setString(2, client.clientId);
    clientPS.setObject(3, toDate(client.dateBirth));
    clientPS.setString(4, client.firstname);
    clientPS.setString(5, client.surname);
    clientPS.setString(6, client.patronymic);
    clientPS.addBatch();
    clientBatchSize++;

    if (maxBatchSize <= clientBatchSize) {
      clientPS.executeBatch();
      connection.commit();
      clientBatchSize = 0;
    }

    System.out.println(contractsResp);
  }

  private static java.sql.Date toDate(Date javaDate) {
    return javaDate == null ? null : new java.sql.Date(javaDate.getTime());
  }


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

  private void finish() throws SQLException {
    if (clientBatchSize > 0) {
      clientPS.executeBatch();
      connection.commit();
      clientBatchSize = 0;
    }
    goContractsResp();
  }

  @Override
  public void close() throws Exception {
    if (clientPS != null) {
      clientPS.close();
      clientPS = null;
    }
  }

  interface CloseBracket {
    void close();
  }

  final List<CloseBracket> closeBracketList = new ArrayList<>();

  ContractsResp contractsResp = null;
  Client client = null;
  long clientNo = 1;

  private void readLine(String line, int lineNo) throws SQLException {
    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.ContractsResp@")) {
      goContractsResp();
      contractsResp = new ContractsResp();
      return;
    }

    if (line.trim().startsWith("client=kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Client@")) {
      client = new Client();
      contractsResp.client = client;
      return;
    }

    {
      int eqIndex = line.indexOf('=');
      if (eqIndex > -1) {
        String key = line.substring(0, eqIndex).trim();
        String value = line.substring(eqIndex + 1).trim();
        if ("<null>".equals(value)) value = null;
        readKeyValue(key, value);
        return;
      }
    }

    if ("]".equals(line.trim())) {
      if (closeBracketList.size() > 0) closeBracketList.remove(closeBracketList.size() - 1).close();
      return;
    }
  }


  boolean inDate = false;
  int year, month, day;

  private Date readDate() {
    GregorianCalendar cal = new GregorianCalendar();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    return cal.getTime();
  }

  private void readKeyValue(String key, String value) {

    if (inDate && "year".equals(key)) {
      year = Integer.parseInt(value);
      return;
    }
    if (inDate && "month".equals(key)) {
      month = Integer.parseInt(value);
      return;
    }
    if (inDate && "day".equals(key)) {
      day = Integer.parseInt(value);
      return;
    }
    if ("dateBirth".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> client.dateBirth = readDate());
      return;
    }
    if ("dateIssuePassport".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> client.dateIssuePassport = readDate());
      return;
    }

    if ("surname".equals(key)) {
      client.surname = value;
      return;
    }
    if ("firstname".equals(key)) {
      client.firstname = value;
      return;
    }
    if ("clientId".equals(key)) {
      client.clientId = value;
      return;
    }


  }

}
