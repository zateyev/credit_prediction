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

    if (line.trim().startsWith("last_pay_date=com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl@")) {
      lastPayDateCal = new GregorianCalendar();
      closeBracketList.add(() -> {
        if (lastPayDateCal == null) return;
        overdue.last_pay_date = lastPayDateCal.getTime();
        lastPayDateCal = null;
      });
      return;
    }

    if (line.trim().startsWith("date_prolongation=com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl@")) {
      dateProlongationCal = new GregorianCalendar();
      closeBracketList.add(() -> {
        if (dateProlongationCal == null) return;
        overdue.date_prolongation = dateProlongationCal.getTime();
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
      overdue.active_summa = new BigDecimal(value);
      return;
    }
    if ("activeSummaNT".equals(key) && overdue != null) {
      overdue.active_summa_nt = new BigDecimal(value);
      return;
    }
    if ("calcPenyDebt".equals(key) && overdue != null) {
      overdue.calc_peny_debt = new BigDecimal(value);
      return;
    }
    if ("calcPenyDebtNT".equals(key) && overdue != null) {
      overdue.calc_peny_debt_nt = new BigDecimal(value);
      return;
    }
    if ("commentFromCFT".equals(key) && overdue != null) {
      overdue.comment_from_cft = value;
      return;
    }
    if ("contractId".equals(key) && overdue != null) {
      overdue.contract_id = value;
      return;
    }
    if ("credExpert".equals(key) && overdue != null) {
      overdue.cred_expert = value;
      return;
    }
    if ("credManagerADUser".equals(key) && overdue != null) {
      overdue.cred_manager_ad_user = value;
      return;
    }
    if ("credManagerDepCode".equals(key) && overdue != null) {
      overdue.cred_manager_dep_code = value;
      return;
    }
    if ("debtAll".equals(key) && overdue != null) {
      overdue.debt_all = new BigDecimal(value);
      return;
    }
    if ("debtAllNT".equals(key) && overdue != null) {
      overdue.debt_all_nt = new BigDecimal(value);
      return;
    }
    if ("debtOnDate".equals(key) && overdue != null) {
      overdue.debt_on_date = new BigDecimal(value);
      return;
    }
    if ("debtOnDateNT".equals(key) && overdue != null) {
      overdue.debt_on_date_nt = new BigDecimal(value);
      return;
    }

    if ("overdueDay".equals(key) && overdue != null) {
      overdue.overdue_day = Integer.parseInt(value);
      return;
    }
    if ("overduePrcDebt".equals(key) && overdue != null) {
      overdue.overdue_prc_debt = new BigDecimal(value);
      return;
    }
    if ("overduePrcDebtNT".equals(key) && overdue != null) {
      overdue.overdue_prc_debt_nt = new BigDecimal(value);
      return;
    }
    if ("planDebtOnDate".equals(key) && overdue != null) {
      overdue.plan_debt_on_date = new BigDecimal(value);
      return;
    }
    if ("planDebtOnDateNT".equals(key) && overdue != null) {
      overdue.plan_debt_on_date_nt = new BigDecimal(value);
      return;
    }
    if ("planPrcDebt".equals(key) && overdue != null) {
      overdue.plan_prc_debt = new BigDecimal(value);
      return;
    }
    if ("planPrcDebtNT".equals(key) && overdue != null) {
      overdue.plan_prc_debt_nt = new BigDecimal(value);
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
//    if ("last_pay_date".equals(key)) {
//      closeBracketList.add(() -> overdue.last_pay_date = readDate());
//      return;
//    }
//    if ("date_prolongation".equals(key)) {
//      closeBracketList.add(() -> overdue.date_prolongation = readDate());
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
      "no, active_summa, active_summa_nt, calc_peny_debt, calc_peny_debt_nt, comment_from_cft, contract_id, cred_expert, cred_manager_ad_user," +
      " cred_manager_dep_code, date_prolongation, debt_all, debt_all_nt, debt_on_date, debt_on_date_nt, last_pay_date, overdue_day," +
      " overdue_prc_debt, overdue_prc_debt_nt, plan_debt_on_date, plan_debt_on_date_nt, plan_prc_debt, plan_prc_debt_nt" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");
  }

  @Override
  protected void createTables() throws SQLException {
    DbAccess.createTable(connection, "create table overdue (" +
      "   no bigint primary key," +
      "   active_summa decimal," +
      "   active_summa_nt decimal," +
      "   calc_peny_debt decimal," +
      "   calc_peny_debt_nt decimal," +
      "   comment_from_cft varchar(300)," +
      "   contract_id varchar(30)," +
      "   cred_expert varchar(300)," +
      "   cred_manager_ad_user varchar(30)," +
      "   cred_manager_dep_code varchar(30)," +
      "   date_prolongation date," +
      "   debt_all decimal," +
      "   debt_all_nt decimal," +
      "   debt_on_date decimal," +
      "   debt_on_date_nt decimal," +
      "   last_pay_date date," +
      "   overdue_day integer," +
      "   overdue_prc_debt decimal," +
      "   overdue_prc_debt_nt decimal," +
      "   plan_debt_on_date decimal," +
      "   plan_debt_on_date_nt decimal," +
      "   plan_prc_debt decimal," +
      "   plan_prc_debt_nt decimal" +
      ")");
  }

  private void addOverdueToBatch() throws SQLException {
    if (overdue == null) return;

    int ind = 1;
    overduePS.setLong(ind++, overdueNo++);
    overduePS.setBigDecimal(ind++, overdue.active_summa);
    overduePS.setBigDecimal(ind++, overdue.active_summa_nt);
    overduePS.setBigDecimal(ind++, overdue.calc_peny_debt);
    overduePS.setBigDecimal(ind++, overdue.calc_peny_debt_nt);
    overduePS.setString(ind++, overdue.comment_from_cft);
    overduePS.setString(ind++, overdue.contract_id);
    overduePS.setString(ind++, overdue.cred_expert);
    overduePS.setString(ind++, overdue.cred_manager_ad_user);
    overduePS.setString(ind++, overdue.cred_manager_dep_code);
    overduePS.setObject(ind++, toDate(overdue.date_prolongation));
    overduePS.setBigDecimal(ind++, overdue.debt_all);
    overduePS.setBigDecimal(ind++, overdue.debt_all_nt);
    overduePS.setBigDecimal(ind++, overdue.debt_on_date);
    overduePS.setBigDecimal(ind++, overdue.debt_on_date_nt);
    overduePS.setObject(ind++, toDate(overdue.last_pay_date));
    overduePS.setInt(ind++, overdue.overdue_day);
    overduePS.setBigDecimal(ind++, overdue.overdue_prc_debt);
    overduePS.setBigDecimal(ind++, overdue.overdue_prc_debt_nt);
    overduePS.setBigDecimal(ind++, overdue.plan_debt_on_date);
    overduePS.setBigDecimal(ind++, overdue.plan_debt_on_date_nt);
    overduePS.setBigDecimal(ind++, overdue.plan_prc_debt);
    overduePS.setBigDecimal(ind, overdue.plan_prc_debt_nt);
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
