package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.overdue.OverduesResp;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class OverduesRespParser extends ParserAbstract {

  private OverduesResp overdue;
  private GregorianCalendar lastPayDateCal;
  private GregorianCalendar dateProlongationCal;
  private PreparedStatement overduePS;
  private int overdueBatchSize = 0;
  private long overdueNo = 1;
//  final List<CloseBracket> closeBracketList = new ArrayList<>();
//  int year, month, day;

  @Override
  protected void readLine(String line, int lineNo) throws SQLException {
    if (line.trim().startsWith("overdue=kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Overdue@")) {
      overdue = new OverduesResp();
      closeBracketList.add(this::addOverdueToBatch);
      return;
    }

    if (line.trim().startsWith("lastPayDate=com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl@")) {
      lastPayDateCal = new GregorianCalendar();
      closeBracketList.add(() -> {
        if (lastPayDateCal == null) return;
        overdue.lastPayDate = lastPayDateCal.getTime();
        lastPayDateCal = null;
      });
      return;
    }

    if (line.trim().startsWith("dateProlongation=com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl@")) {
      dateProlongationCal = new GregorianCalendar();
      closeBracketList.add(() -> {
        if (dateProlongationCal == null) return;
        overdue.dateProlongation = dateProlongationCal.getTime();
        dateProlongationCal = null;
      });
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

  @Override
  protected void readKeyValue(String key, String value) {
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
      int year = Integer.parseInt(value);
      if (lastPayDateCal != null) {
        lastPayDateCal.set(Calendar.YEAR, year);
        return;
      }
      if (dateProlongationCal != null) {
        dateProlongationCal.set(Calendar.YEAR, year);
        return;
      }
//      year = Integer.parseInt(value);
    }
    if ("month".equals(key)) {
      int month = Integer.parseInt(value);
      if (lastPayDateCal != null) {
        lastPayDateCal.set(Calendar.MONTH, month);
        return;
      }
      if (dateProlongationCal != null) {
        dateProlongationCal.set(Calendar.MONTH, month);
        return;
      }
//      month = Integer.parseInt(value);
//      return;
    }
    if ("day".equals(key)) {
      int day = Integer.parseInt(value);
      if (lastPayDateCal != null) {
        lastPayDateCal.set(Calendar.DAY_OF_MONTH, day);
        return;
      }
      if (dateProlongationCal != null) {
        dateProlongationCal.set(Calendar.DAY_OF_MONTH, day);
        return;
      }
//      day = Integer.parseInt(value);
//      return;
    }
//    if ("lastPayDate".equals(key)) {
//      closeBracketList.add(() -> overdue.lastPayDate = readDate());
//      return;
//    }
//    if ("dateProlongation".equals(key)) {
//      closeBracketList.add(() -> overdue.dateProlongation = readDate());
//      return;
//    }
  }

  @Override
  protected void finish() throws SQLException {
    if (overdueBatchSize > 0) {
      overduePS.executeBatch();
      overdueBatchSize = 0;
    }
    connection.commit();
  }

  public OverduesRespParser(Connection connection, int maxBatchSize) throws SQLException {
    super(connection, maxBatchSize);
    createTables();

    connection.setAutoCommit(false);

    overduePS = connection.prepareStatement("insert into overdue (" +
      "no, activeSumma, activeSummaNT, calcPenyDebt, calcPenyDebtNT, commentFromCFT, contractId, credExpert, credManagerADUser," +
      " credManagerDepCode, dateProlongation, debtAll, debtAllNT, debtOnDate, debtOnDateNT, lastPayDate, overdueDay," +
      " overduePrcDebt, overduePrcDebtNT, planDebtOnDate, planDebtOnDateNT, planPrcDebt, planPrcDebtNT" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");
  }

  @Override
  protected void createTables() throws SQLException {
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
      "   dateProlongation date," +
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
    overduePS.setObject(ind++, toDate(overdue.dateProlongation));
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

  @Override
  public void close() throws Exception {
    if (overduePS != null) {
      overduePS.close();
      overduePS = null;
    }
    connection.setAutoCommit(true);
  }
}
