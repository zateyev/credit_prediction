package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.contract.Client;
import kz.greetgo.credit_prediction.prepare_input_data.model.contract.ContractsResp;
import kz.greetgo.credit_prediction.prepare_input_data.model.contract.Credit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class ContractsRespParser implements AutoCloseable {

  PreparedStatement clientPS;
  PreparedStatement creditPS;
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
      "  patronymic varchar(300)," +
      "  inn varchar(50)," +
      "  numSeriaPassport varchar(50)," +
      "  sex varchar(20)," +
      "  type varchar(30)," +
      "  factAddress varchar(300)," +
      "  regAddress varchar(300)," +
      "  typePassport varchar(30)," +
      "  whoIssuePassport varchar(300)" +
      ")");
    DbAccess.createTable(connection, "create table credit (" +
      "  no         bigint primary key," +
      "  clientId   varchar(20)," +
      "  branch  varchar(300)," +
      "  branchCode  varchar(300)," +
      "  contractManager    varchar(300)," +
      "  contractManagerADUser varchar(300)," +
      "  credLineId varchar(50)," +
      "  departCode varchar(50)," +
      "  departName varchar(300)," +
      "  dogSumma decimal," +
      "  dogSummaNT decimal," +
      "  gracePeriod int," +
      "  groupConvNum varchar(30)," +
      "  kindCredit varchar(50)," +
      "  methodCalcPrc varchar(50)," +
      "  nameGroupClient varchar(50)," +
      "  numDog varchar(50)," +
      "  numDogCredLine varchar(50)," +
      "  podSectorCred varchar(300)," +
      "  prcRate decimal," +
      "  prePaymentAcc varchar(300)," +
      "  product varchar(300)," +
      "  rateAdminPrc decimal," +
      "  sectorCred varchar(300)," +
      "  stupenCred int," +
      "  sumAdminPrc decimal," +
      "  sumAdminPrcNT decimal," +
      "  sumCredLine decimal," +
      "  valuta varchar(20)," +
      "  dateBegin  date," +
      "  dateEnd  date," +
      "  dateOpen  date" +
      ")");

    connection.setAutoCommit(false);

    clientPS = connection.prepareStatement("insert into client (" +
      "no, clientId, dateBirth, firstname, surname, patronymic, inn, numSeriaPassport, sex, type, factAddress, regAddress, " +
      "typePassport, whoIssuePassport" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");

    creditPS = connection.prepareStatement("insert into credit (" +
      "no, clientId, branch, branchCode, contractManager, contractManagerADUser, credLineId, departCode, departName, " +
      "dogSumma, dogSummaNT, gracePeriod, groupConvNum, kindCredit, methodCalcPrc, nameGroupClient, numDog, numDogCredLine, " +
      "podSectorCred, prcRate, prePaymentAcc, product, rateAdminPrc, sectorCred, stupenCred, sumAdminPrc, sumAdminPrcNT, " +
      "sumCredLine, valuta, dateBegin, dateEnd, dateOpen" + //32
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");
  }

  int clientBatchSize = 0;
  int creditBatchSize = 0;

  private void goContractsResp() throws SQLException {
    if (contractsResp == null) return;

    int ind = 1;
    clientPS.setLong(ind++, clientNo++);
    clientPS.setString(ind++, client.clientId);
    clientPS.setObject(ind++, toDate(client.dateBirth));
    clientPS.setString(ind++, client.firstname);
    clientPS.setString(ind++, client.surname);
    clientPS.setString(ind++, client.patronymic);
    clientPS.setString(ind++, client.inn);
    clientPS.setString(ind++, client.numSeriaPassport);
    clientPS.setString(ind++, client.sex);
    clientPS.setString(ind++, client.type);
    clientPS.setString(ind++, client.factAddress);
    clientPS.setString(ind++, client.regAddress);
    clientPS.setString(ind++, client.typePassport);
    clientPS.setString(ind, client.whoIssuePassport);
    clientPS.addBatch();
    clientBatchSize++;

    if (maxBatchSize <= clientBatchSize) {
      clientPS.executeBatch();
      connection.commit();
      clientBatchSize = 0;
    }

    if (credit == null) return;

    ind = 1;
    creditPS.setLong(ind++, clientNo++);
    creditPS.setString(ind++, credit.clientId);
    creditPS.setString(ind++, credit.branch);
    creditPS.setString(ind++, credit.branchCode);
    creditPS.setString(ind++, credit.contractManager);
    creditPS.setString(ind++, credit.contractManagerADUser);
    creditPS.setString(ind++, credit.credLineId);
    creditPS.setString(ind++, credit.departCode);
    creditPS.setString(ind++, credit.departName);
    creditPS.setBigDecimal(ind++, credit.dogSumma);
    creditPS.setBigDecimal(ind++, credit.dogSummaNT);
    creditPS.setInt(ind++, credit.gracePeriod);
    creditPS.setString(ind++, credit.groupConvNum);
    creditPS.setString(ind++, credit.kindCredit);
    creditPS.setString(ind++, credit.methodCalcPrc);
    creditPS.setString(ind++, credit.nameGroupClient);
    creditPS.setString(ind++, credit.numDog);
    creditPS.setString(ind++, credit.numDogCredLine);
    creditPS.setString(ind++, credit.podSectorCred);
    creditPS.setBigDecimal(ind++, credit.prcRate);
    creditPS.setString(ind++, credit.prePaymentAcc);
    creditPS.setString(ind++, credit.product);
    creditPS.setBigDecimal(ind++, credit.rateAdminPrc);
    creditPS.setString(ind++, credit.sectorCred);
    creditPS.setInt(ind++, credit.stupenCred);
    creditPS.setBigDecimal(ind++, credit.sumAdminPrc);
    creditPS.setBigDecimal(ind++, credit.sumAdminPrcNT);
    creditPS.setBigDecimal(ind++, credit.sumCredLine);
    creditPS.setString(ind++, credit.valuta);
    creditPS.setObject(ind++, toDate(credit.dateBegin));
    creditPS.setObject(ind++, toDate(credit.dateEnd));
    creditPS.setObject(ind, toDate(credit.dateOpen));
    creditPS.addBatch();
    creditBatchSize++;

    if (maxBatchSize <= creditBatchSize) {
      creditPS.executeBatch();
      connection.commit();
      creditBatchSize = 0;
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
      clientBatchSize = 0;
    }
    if (creditBatchSize > 0) {
      creditPS.executeBatch();
      creditBatchSize = 0;
    }
    connection.commit();
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
  Credit credit = null;
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

    if (line.trim().startsWith("credit=kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Credit@")) {
      credit = new Credit();
      contractsResp.credit = credit;
//      closeBracketList.add(() -> credit = null);
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
    if ("dateBegin".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> credit.dateBegin = readDate());
      return;
    }
    if ("dateEnd".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> credit.dateEnd = readDate());
      return;
    }
    if ("dateOpen".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> credit.dateOpen = readDate());
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
    if ("patronymic".equals(key)) {
      client.patronymic = value;
      return;
    }
    if ("inn".equals(key)) {
      client.inn = value;
      return;
    }
    if ("numSeriaPassport".equals(key)) {
      client.numSeriaPassport = value;
      return;
    }
    if ("sex".equals(key)) {
      client.sex = value;
      return;
    }
    if ("type".equals(key)) {
      client.type = value;
      return;
    }
    if ("factAddress".equals(key)) {
      client.factAddress = value;
      return;
    }
    if ("regAddress".equals(key)) {
      client.regAddress = value;
      return;
    }
    if ("typePassport".equals(key)) {
      client.typePassport = value;
      return;
    }
    if ("whoIssuePassport".equals(key)) {
      client.whoIssuePassport = value;
      return;
    }
    if ("clientId".equals(key)) {
      client.clientId = value;
      return;
    }


    // read credit fields
    if ("branch".equals(key)) {
      credit.branch = value;
      return;
    }
    if ("branchCode".equals(key)) {
      credit.branchCode = value;
      return;
    }
    if ("contractId".equals(key) && credit != null) {
      credit.contractId = value;
      return;
    }
    if ("contractManager".equals(key) && credit != null) {
      credit.contractManager = value;
      return;
    }
    if ("contractManagerADUser".equals(key) && credit != null) {
      credit.contractManagerADUser = value;
      return;
    }
    if ("credLineId".equals(key) && credit != null) {
      credit.credLineId = value;
      return;
    }
    if ("departCode".equals(key) && credit != null) {
      credit.departCode = value;
      return;
    }
    if ("departName".equals(key) && credit != null) {
      credit.departName = value;
      return;
    }
    if ("dogSumma".equals(key) && credit != null) {
      credit.dogSumma = new BigDecimal(value);
      return;
    }
    if ("dogSummaNT".equals(key) && credit != null) {
      credit.dogSummaNT = new BigDecimal(value);
      return;
    }
    if ("gracePeriod".equals(key) && credit != null) {
      credit.gracePeriod = Integer.parseInt(value);
      return;
    }
    if ("groupConvNum".equals(key) && credit != null) {
      credit.groupConvNum = value;
      return;
    }
    if ("kindCredit".equals(key) && credit != null) {
      credit.kindCredit = value;
      return;
    }
    if ("methodCalcPrc".equals(key) && credit != null) {
      credit.methodCalcPrc = value;
      return;
    }
    if ("nameGroupClient".equals(key) && credit != null) {
      credit.nameGroupClient = value;
      return;
    }
    if ("numDog".equals(key) && credit != null) {
      credit.numDog = value;
      return;
    }
    if ("numDogCredLine".equals(key) && credit != null) {
      credit.numDogCredLine = value;
      return;
    }
    if ("podSectorCred".equals(key) && credit != null) {
      credit.podSectorCred = value;
      return;
    }
    if ("prcRate".equals(key) && credit != null) {
      credit.prcRate = new BigDecimal(value);
      return;
    }
    if ("prePaymentAcc".equals(key) && credit != null) {
      credit.prePaymentAcc = value;
      return;
    }
    if ("product".equals(key) && credit != null) {
      credit.product = value;
      return;
    }
    if ("rateAdminPrc".equals(key) && credit != null) {
      credit.rateAdminPrc = new BigDecimal(value);
      return;
    }
    if ("sectorCred".equals(key) && credit != null) {
      credit.sectorCred = value;
      return;
    }
    if ("stupenCred".equals(key) && credit != null) {
      credit.stupenCred = Integer.parseInt(value);
      return;
    }
    if ("sumAdminPrc".equals(key) && credit != null) {
      credit.sumAdminPrc = new BigDecimal(value);
      return;
    }
    if ("sumAdminPrcNT".equals(key) && credit != null) {
      credit.sumAdminPrcNT = new BigDecimal(value);
      return;
    }
    if ("sumCredLine".equals(key) && credit != null) {
      credit.sumCredLine = new BigDecimal(value);
      return;
    }
    if ("valuta".equals(key) && credit != null) {
      credit.valuta = value;
      return;
    }

  }

}
