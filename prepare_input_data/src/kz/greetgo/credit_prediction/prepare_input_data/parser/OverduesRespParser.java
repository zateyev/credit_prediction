package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.overdue.OverduesResp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class OverduesRespParser implements AutoCloseable {

  private final Connection connection;
  private final int maxBatchSize;

  private OverduesResp overdue;
  private PreparedStatement overduePS;
  private int overdueBatchSize = 0;
  private long overdueNo = 1;
  final List<CloseBracket> closeBracketList = new ArrayList<>();
  int year, month, day;

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

  private void readLine(String line, int lineNo) throws SQLException {
    if (line.trim().startsWith("overdue=kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Overdue@")) {
      overdue = new OverduesResp();
      closeBracketList.add(this::addOverdueToBatch);
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

  private void readKeyValue(String key, String value) {
    if ("activeSumma".equals(key) && overdue != null) {
      overdue.activeSumma = new BigDecimal(value);
      return;
    }
    if ("activeSummaNT".equals(key) && overdue != null) {
      overdue.activeSummaNT = new BigDecimal(value);
      return;
    }
    if ("calcPenyDebt".equals(key) && overdue != null) {
      overdue.calcPenyDebt = new BigDecimal(value);
      return;
    }
    if ("calcPenyDebtNT".equals(key) && overdue != null) {
      overdue.calcPenyDebtNT = new BigDecimal(value);
      return;
    }
    if ("commentFromCFT".equals(key) && overdue != null) {
      overdue.commentFromCFT = value;
      return;
    }
    if ("contractId".equals(key) && overdue != null) {
      overdue.contractId = value;
      return;
    }
    if ("credExpert".equals(key) && overdue != null) {
      overdue.credExpert = value;
      return;
    }
    if ("credManagerADUser".equals(key) && overdue != null) {
      overdue.credManagerADUser = value;
      return;
    }
    if ("credManagerDepCode".equals(key) && overdue != null) {
      overdue.credManagerDepCode = value;
      return;
    }
    if ("dateProlongation".equals(key) && overdue != null) {
      overdue.dateProlongation = value;
      return;
    }
    if ("debtAll".equals(key) && overdue != null) {
      overdue.debtAll = new BigDecimal(value);
      return;
    }
    if ("debtAllNT".equals(key) && overdue != null) {
      overdue.debtAllNT = new BigDecimal(value);
      return;
    }
    if ("debtOnDate".equals(key) && overdue != null) {
      overdue.debtOnDate = new BigDecimal(value);
      return;
    }
    if ("debtOnDateNT".equals(key) && overdue != null) {
      overdue.debtOnDateNT = new BigDecimal(value);
      return;
    }

    if ("overdueDay".equals(key) && overdue != null) {
      overdue.overdueDay = Integer.parseInt(value);
      return;
    }
    if ("overduePrcDebt".equals(key) && overdue != null) {
      overdue.overduePrcDebt = new BigDecimal(value);
      return;
    }
    if ("overduePrcDebtNT".equals(key) && overdue != null) {
      overdue.overduePrcDebtNT = new BigDecimal(value);
      return;
    }
    if ("planDebtOnDate".equals(key) && overdue != null) {
      overdue.planDebtOnDate = new BigDecimal(value);
      return;
    }
    if ("planDebtOnDateNT".equals(key) && overdue != null) {
      overdue.planDebtOnDateNT = new BigDecimal(value);
      return;
    }
    if ("planPrcDebt".equals(key) && overdue != null) {
      overdue.planPrcDebt = new BigDecimal(value);
      return;
    }
    if ("planPrcDebtNT".equals(key) && overdue != null) {
      overdue.planPrcDebtNT = new BigDecimal(value);
      return;
    }

    if ("year".equals(key)) {
      year = Integer.parseInt(value);
      return;
    }
    if ("month".equals(key)) {
      month = Integer.parseInt(value);
      return;
    }
    if ("day".equals(key)) {
      day = Integer.parseInt(value);
      return;
    }
    if ("lastPayDate".equals(key)) {
      closeBracketList.add(() -> overdue.lastPayDate = readDate());
      return;
    }
  }

  private Date readDate() {
    GregorianCalendar cal = new GregorianCalendar();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day);
    return cal.getTime();
  }

  private void finish() throws SQLException {
    if (overdueBatchSize > 0) {
      overduePS.executeBatch();
      overdueBatchSize = 0;
    }
    connection.commit();
  }

  public OverduesRespParser(Connection connection, int maxBatchSize) throws SQLException {
    this.connection = connection;
    this.maxBatchSize = maxBatchSize;

    DbAccess.createTable(connection, "create table overdue (" +
      "   no bigint primary key," +
      "   activeSumma decimal," +
      "   activeSummaNT decimal," +
      "   calcPenyDebt decimal," +
      "   calcPenyDebtNT decimal," +
      "   commentFromCFT varchar(300)," +
      "   contractId varchar(30)," +
      "   credExpert varchar(300)," +
      "   credManagerADUser varchar(30)," +
      "   credManagerDepCode varchar(30)," +
      "   dateProlongation varchar(30)," +
      "   debtAll decimal," +
      "   debtAllNT decimal," +
      "   debtOnDate decimal," +
      "   debtOnDateNT decimal," +
      "   lastPayDate date," +
      "   overdueDay integer," +
      "   overduePrcDebt decimal," +
      "   overduePrcDebtNT decimal," +
      "   planDebtOnDate decimal," +
      "   planDebtOnDateNT decimal," +
      "   planPrcDebt decimal," +
      "   planPrcDebtNT decimal" +
      ")");

    connection.setAutoCommit(false);

    overduePS = connection.prepareStatement("insert into overdue (" +
      "no, activeSumma, activeSummaNT, calcPenyDebt, calcPenyDebtNT, commentFromCFT, contractId, credExpert, credManagerADUser," +
      " credManagerDepCode, dateProlongation, debtAll, debtAllNT, debtOnDate, debtOnDateNT, lastPayDate, overdueDay," +
      " overduePrcDebt, overduePrcDebtNT, planDebtOnDate, planDebtOnDateNT, planPrcDebt, planPrcDebtNT" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");
  }

  private void addOverdueToBatch() throws SQLException {
    if (overdue == null) return;

    int ind = 1;
    overduePS.setLong(ind++, overdueNo++);
    overduePS.setBigDecimal(ind++, overdue.activeSumma);
    overduePS.setBigDecimal(ind++, overdue.activeSummaNT);
    overduePS.setBigDecimal(ind++, overdue.calcPenyDebt);
    overduePS.setBigDecimal(ind++, overdue.calcPenyDebtNT);
    overduePS.setString(ind++, overdue.commentFromCFT);
    overduePS.setString(ind++, overdue.contractId);
    overduePS.setString(ind++, overdue.credExpert);
    overduePS.setString(ind++, overdue.credManagerADUser);
    overduePS.setString(ind++, overdue.credManagerDepCode);
    overduePS.setString(ind++, overdue.dateProlongation);
    overduePS.setBigDecimal(ind++, overdue.debtAll);
    overduePS.setBigDecimal(ind++, overdue.debtAllNT);
    overduePS.setBigDecimal(ind++, overdue.debtOnDate);
    overduePS.setBigDecimal(ind++, overdue.debtOnDateNT);
    overduePS.setObject(ind++, toDate(overdue.lastPayDate));
    overduePS.setInt(ind++, overdue.overdueDay);
    overduePS.setBigDecimal(ind++, overdue.overduePrcDebt);
    overduePS.setBigDecimal(ind++, overdue.overduePrcDebtNT);
    overduePS.setBigDecimal(ind++, overdue.planDebtOnDate);
    overduePS.setBigDecimal(ind++, overdue.planDebtOnDateNT);
    overduePS.setBigDecimal(ind++, overdue.planPrcDebt);
    overduePS.setBigDecimal(ind, overdue.planPrcDebtNT);
    overduePS.addBatch();
    overdueBatchSize++;

    if (maxBatchSize <= overdueBatchSize) {
      overduePS.executeBatch();
      connection.commit();
      overdueBatchSize = 0;
    }

    overdue = null;
  }

  private static java.sql.Date toDate(Date javaDate) {
    return javaDate == null ? null : new java.sql.Date(javaDate.getTime());
  }

  @Override
  public void close() throws Exception {
    if (overduePS != null) {
      overduePS.close();
      overduePS = null;
    }
    connection.setAutoCommit(true);
  }
}
