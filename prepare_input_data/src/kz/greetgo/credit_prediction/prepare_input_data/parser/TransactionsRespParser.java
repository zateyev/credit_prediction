package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.transaction.AccMove;
import kz.greetgo.credit_prediction.prepare_input_data.model.transaction.FactOper;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class TransactionsRespParser extends ParserAbstract {

  private FactOper factOper;
  private PreparedStatement factOperPS;
  private GregorianCalendar operDateCalF;
  private int factOperBatchSize = 0;

  private long factOperNo = 1;
  private AccMove accMove;
  private PreparedStatement accMovePS;
  private GregorianCalendar operDateCal;
  private int accMoveBatchSize = 0;
  private long accMoveNo = 1;

  @Override
  protected void readLine(String line, int lineNo) throws SQLException {
    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.FactOper@")) {
      factOper = new FactOper();
      closeBracketList.add(this::addFactOperToBatch);
      return;
    }

    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.AccMove@")) {
      accMove = new AccMove();
      closeBracketList.add(this::addAccMoveToBatch);
      return;
    }

    if (line.trim().startsWith("oper_date=com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl@")) {
      if (factOper != null) {
        operDateCalF = new GregorianCalendar();
        closeBracketList.add(() -> {
          factOper.oper_date = operDateCalF.getTime();
          operDateCalF = null;
        });
        return;
      }
      if (accMove != null) {
        operDateCal = new GregorianCalendar();
        closeBracketList.add(() -> {
          accMove.oper_date = operDateCal.getTime();
          operDateCal = null;
        });
        return;
      }
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
    if ("contractId".equals(key) && factOper != null) {
      factOper.contract_id = value;
      return;
    }
    if ("overdueCred".equals(key) && factOper != null) {
      factOper.overdue_cred = new BigDecimal(value);
      return;
    }
    if ("overdueCredNT".equals(key) && factOper != null) {
      factOper.overdue_cred_nt = new BigDecimal(value);
      return;
    }
    if ("overduePrc".equals(key) && factOper != null) {
      factOper.overdue_prc = new BigDecimal(value);
      return;
    }
    if ("overduePrcNT".equals(key) && factOper != null) {
      factOper.overdue_prc_nt = new BigDecimal(value);
      return;
    }
    if ("payCred".equals(key) && factOper != null) {
      factOper.pay_cred = new BigDecimal(value);
      return;
    }
    if ("payCredNT".equals(key) && factOper != null) {
      factOper.pay_cred_nt = new BigDecimal(value);
      return;
    }
    if ("payPrc".equals(key) && factOper != null) {
      factOper.pay_prc = new BigDecimal(value);
      return;
    }
    if ("payPrc112".equals(key) && factOper != null) {
      factOper.pay_prc_112 = new BigDecimal(value);
      return;
    }
    if ("payPrc112NT".equals(key) && factOper != null) {
      factOper.pay_prc_112_nt = new BigDecimal(value);
      return;
    }

    if ("payPrcNT".equals(key) && factOper != null) {
      factOper.pay_prc_nt = new BigDecimal(value);
      return;
    }
    if ("penyCalcBalance".equals(key) && factOper != null) {
      factOper.peny_calc_balance = new BigDecimal(value);
      return;
    }
    if ("penyCalcBalanceNT".equals(key) && factOper != null) {
      factOper.peny_calc_balance_nt = new BigDecimal(value);
      return;
    }
    if ("penyPay".equals(key) && factOper != null) {
      factOper.peny_pay = new BigDecimal(value);
      return;
    }
    if ("penyPayNT".equals(key) && factOper != null) {
      factOper.peny_pay_nt = new BigDecimal(value);
      return;
    }
    if ("prc112Balance".equals(key) && factOper != null) {
      factOper.prc_112_balance = new BigDecimal(value);
      return;
    }
    if ("prc112BalanceNT".equals(key) && factOper != null) {
      factOper.prc_112_balance_nt = new BigDecimal(value);
      return;
    }
    if ("valuta".equals(key) && factOper != null) {
      factOper.valuta = value;
      return;
    }

    if ("year".equals(key)) {
      int year = Integer.parseInt(value);
      if (operDateCal != null) {
        operDateCal.set(Calendar.YEAR, year);
        return;
      }
      if (operDateCalF != null) {
        operDateCalF.set(Calendar.YEAR, year);
        return;
      }
//      year = Integer.parseInt(value);
//      return;
    }
    if ("month".equals(key)) {
      int month = Integer.parseInt(value);
      if (operDateCal != null) {
        operDateCal.set(Calendar.MONTH, month);
        return;
      }
      if (operDateCalF != null) {
        operDateCalF.set(Calendar.MONTH, month);
        return;
      }
//      month = Integer.parseInt(value);
//      return;
    }
    if ("day".equals(key)) {
      int day = Integer.parseInt(value);
      if (operDateCal != null) {
        operDateCal.set(Calendar.DAY_OF_MONTH, day);
        return;
      }
      if (operDateCalF != null) {
        operDateCalF.set(Calendar.DAY_OF_MONTH, day);
        return;
      }
//      day = Integer.parseInt(value);
//      return;
    }
    if ("operDate".equals(key) && factOper != null) {
      closeBracketList.add(() -> factOper.oper_date = readDate());
      return;
    }

    // read AccMove
    if ("accCorr".equals(key) && accMove != null) {
      accMove.acc_corr = value;
      return;
    }
    if ("accMoveId".equals(key) && accMove != null) {
      accMove.acc_move_id = value;
      return;
    }
    if ("accNum".equals(key) && accMove != null) {
      accMove.acc_num = value;
      return;
    }
    if ("accType".equals(key) && accMove != null) {
      accMove.acc_type = value;
      return;
    }
    if ("closeBalance".equals(key) && accMove != null) {
      accMove.close_balance = new BigDecimal(value);
      return;
    }
    if ("closeBalanceNT".equals(key) && accMove != null) {
      accMove.close_balance_nt = new BigDecimal(value);
      return;
    }
    if ("contractId".equals(key) && accMove != null) {
      accMove.contract_id = value;
      return;
    }
    if ("openBalance".equals(key) && accMove != null) {
      accMove.open_balance = new BigDecimal(value);
      return;
    }
    if ("openBalanceNT".equals(key) && accMove != null) {
      accMove.open_balance_nt = new BigDecimal(value);
      return;
    }
    if ("operDate".equals(key) && accMove != null) {
      closeBracketList.add(() -> accMove.oper_date = readDate());
      return;
    }
    if ("turnCred".equals(key) && accMove != null) {
      accMove.turn_cred = new BigDecimal(value);
      return;
    }
    if ("turnCredNT".equals(key) && accMove != null) {
      accMove.turn_cred_nt = new BigDecimal(value);
      return;
    }
    if ("turnDebt".equals(key) && accMove != null) {
      accMove.turn_debt = new BigDecimal(value);
      return;
    }
    if ("turnDebtNT".equals(key) && accMove != null) {
      accMove.turn_debt_nt = new BigDecimal(value);
      return;
    }
    if ("valuta".equals(key) && accMove != null) {
      accMove.valuta = value;
      return;
    }
  }

  @Override
  protected void finish() throws SQLException {
    if (factOperBatchSize > 0) {
      factOperPS.executeBatch();
      factOperBatchSize = 0;
    }
    if (accMoveBatchSize > 0) {
      accMovePS.executeBatch();
      accMoveBatchSize = 0;
    }
    connection.commit();
  }

  public TransactionsRespParser(Connection connection, int maxBatchSize) throws SQLException {
    super(connection, maxBatchSize);

    createTables();

    connection.setAutoCommit(false);

    factOperPS = connection.prepareStatement("insert into fact_oper (" +
      "no, contract_id, oper_date, overdue_cred, overdue_cred_nt, overdue_prc, overdue_prc_nt, pay_cred, pay_cred_nt, pay_prc, " +
      " pay_prc_112, pay_prc_112_nt, pay_prc_nt, peny_calc_balance, peny_calc_balance_nt, peny_pay, peny_pay_nt, prc_112_balance, " +
      " prc_112_balance_nt, valuta" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");

    accMovePS = connection.prepareStatement("insert into acc_move (" +
      "no, acc_corr, acc_move_id, acc_num, acc_type, close_balance, close_balance_nt, contract_id, open_balance, open_balance_nt, " +
      " oper_date, turn_cred, turn_cred_nt, turn_debt, turn_debt_nt, valuta" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");
  }

  @Override
  protected void createTables() throws SQLException {
    DbAccess.createTable(connection, "create table fact_oper (" +
      "   no bigint primary key," +
      "   contract_id varchar(20)," +
      "   oper_date date," +
      "   overdue_cred decimal," +
      "   overdue_cred_nt decimal," +
      "   overdue_prc decimal," +
      "   overdue_prc_nt decimal," +
      "   pay_cred decimal," +
      "   pay_cred_nt decimal," +
      "   pay_prc decimal," +
      "   pay_prc_112 decimal," +
      "   pay_prc_112_nt decimal," +
      "   pay_prc_nt decimal," +
      "   peny_calc_balance decimal," +
      "   peny_calc_balance_nt decimal," +
      "   peny_pay decimal," +
      "   peny_pay_nt decimal," +
      "   prc_112_balance decimal," +
      "   prc_112_balance_nt decimal," +
      "   valuta varchar(20)" +
      ")");

    DbAccess.createTable(connection, "create table acc_move (" +
      "   no bigint primary key," +
      "   acc_corr varchar(30)," +
      "   acc_move_id varchar(30)," +
      "   acc_num varchar(30)," +
      "   acc_type varchar(30)," +
      "   close_balance decimal," +
      "   close_balance_nt decimal," +
      "   contract_id varchar(20)," +
      "   open_balance decimal," +
      "   open_balance_nt decimal," +
      "   oper_date Date," +
      "   turn_cred decimal," +
      "   turn_cred_nt decimal," +
      "   turn_debt decimal," +
      "   turn_debt_nt decimal," +
      "   valuta varchar(20)" +
      ")");
  }

  private void addFactOperToBatch() throws SQLException {
    if (factOper == null) return;

    int ind = 1;
    factOperPS.setLong(ind++, factOperNo++);
    factOperPS.setString(ind++, factOper.contract_id);
    factOperPS.setObject(ind++, toDate(factOper.oper_date));
    factOperPS.setBigDecimal(ind++, factOper.overdue_cred);
    factOperPS.setBigDecimal(ind++, factOper.overdue_cred_nt);
    factOperPS.setBigDecimal(ind++, factOper.overdue_prc);
    factOperPS.setBigDecimal(ind++, factOper.overdue_cred_nt);
    factOperPS.setBigDecimal(ind++, factOper.pay_cred);
    factOperPS.setBigDecimal(ind++, factOper.pay_cred_nt);
    factOperPS.setBigDecimal(ind++, factOper.pay_prc);
    factOperPS.setBigDecimal(ind++, factOper.pay_prc_112);
    factOperPS.setBigDecimal(ind++, factOper.pay_prc_112_nt);
    factOperPS.setBigDecimal(ind++, factOper.pay_prc_nt);
    factOperPS.setBigDecimal(ind++, factOper.peny_calc_balance);
    factOperPS.setBigDecimal(ind++, factOper.peny_calc_balance_nt);
    factOperPS.setBigDecimal(ind++, factOper.peny_pay);
    factOperPS.setBigDecimal(ind++, factOper.peny_pay_nt);
    factOperPS.setBigDecimal(ind++, factOper.prc_112_balance);
    factOperPS.setBigDecimal(ind++, factOper.prc_112_balance_nt);
    factOperPS.setString(ind, factOper.valuta);
    factOperPS.addBatch();
    factOperBatchSize++;

    if (maxBatchSize <= factOperBatchSize) {
      factOperPS.executeBatch();
      connection.commit();
      factOperBatchSize = 0;
    }

    factOper = null;
  }

  private void addAccMoveToBatch() throws SQLException {
    if (accMove == null) return;

    int ind = 1;
    accMovePS.setLong(ind++, accMoveNo++);
    accMovePS.setString(ind++, accMove.acc_corr);
    accMovePS.setString(ind++, accMove.acc_move_id);
    accMovePS.setString(ind++, accMove.acc_num);
    accMovePS.setString(ind++, accMove.acc_type);
    accMovePS.setBigDecimal(ind++, accMove.close_balance);
    accMovePS.setBigDecimal(ind++, accMove.close_balance_nt);
    accMovePS.setString(ind++, accMove.contract_id);
    accMovePS.setBigDecimal(ind++, accMove.open_balance);
    accMovePS.setBigDecimal(ind++, accMove.open_balance_nt);
    accMovePS.setObject(ind++, toDate(accMove.oper_date));
    accMovePS.setBigDecimal(ind++, accMove.turn_cred);
    accMovePS.setBigDecimal(ind++, accMove.turn_cred_nt);
    accMovePS.setBigDecimal(ind++, accMove.turn_debt);
    accMovePS.setBigDecimal(ind++, accMove.turn_debt_nt);
    accMovePS.setString(ind, accMove.valuta);
    accMovePS.addBatch();
    accMoveBatchSize++;

    if (maxBatchSize <= accMoveBatchSize) {
      accMovePS.executeBatch();
      connection.commit();
      accMoveBatchSize = 0;
    }

    accMove = null;
  }

  @Override
  public void close() throws Exception {
    if (factOperPS != null) {
      factOperPS.close();
      factOperPS = null;
    }
    if (accMovePS != null) {
      accMovePS.close();
      accMovePS = null;
    }
    connection.setAutoCommit(true);
  }
}
