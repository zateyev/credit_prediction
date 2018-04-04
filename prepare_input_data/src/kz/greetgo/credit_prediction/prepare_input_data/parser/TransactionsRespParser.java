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

    if (line.trim().startsWith("operDate=com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl@")) {
      if (factOper != null) {
        operDateCalF = new GregorianCalendar();
        closeBracketList.add(() -> {
          factOper.operDate = operDateCalF.getTime();
          operDateCalF = null;
        });
        return;
      }
      if (accMove != null) {
        operDateCal = new GregorianCalendar();
        closeBracketList.add(() -> {
          accMove.operDate = operDateCal.getTime();
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
      factOper.contractId = value;
      return;
    }
    if ("overdueCred".equals(key) && factOper != null) {
      factOper.overdueCred = new BigDecimal(value);
      return;
    }
    if ("overdueCredNT".equals(key) && factOper != null) {
      factOper.overdueCredNT = new BigDecimal(value);
      return;
    }
    if ("overduePrc".equals(key) && factOper != null) {
      factOper.overduePrc = new BigDecimal(value);
      return;
    }
    if ("overduePrcNT".equals(key) && factOper != null) {
      factOper.overduePrcNT = new BigDecimal(value);
      return;
    }
    if ("payCred".equals(key) && factOper != null) {
      factOper.payCred = new BigDecimal(value);
      return;
    }
    if ("payCredNT".equals(key) && factOper != null) {
      factOper.payCredNT = new BigDecimal(value);
      return;
    }
    if ("payPrc".equals(key) && factOper != null) {
      factOper.payPrc = new BigDecimal(value);
      return;
    }
    if ("payPrc112".equals(key) && factOper != null) {
      factOper.payPrc112 = new BigDecimal(value);
      return;
    }
    if ("payPrc112NT".equals(key) && factOper != null) {
      factOper.payPrc112NT = new BigDecimal(value);
      return;
    }

    if ("payPrcNT".equals(key) && factOper != null) {
      factOper.payPrcNT = new BigDecimal(value);
      return;
    }
    if ("penyCalcBalance".equals(key) && factOper != null) {
      factOper.penyCalcBalance = new BigDecimal(value);
      return;
    }
    if ("penyCalcBalanceNT".equals(key) && factOper != null) {
      factOper.penyCalcBalanceNT = new BigDecimal(value);
      return;
    }
    if ("penyPay".equals(key) && factOper != null) {
      factOper.penyPay = new BigDecimal(value);
      return;
    }
    if ("penyPayNT".equals(key) && factOper != null) {
      factOper.penyPayNT = new BigDecimal(value);
      return;
    }
    if ("prc112Balance".equals(key) && factOper != null) {
      factOper.prc112Balance = new BigDecimal(value);
      return;
    }
    if ("prc112BalanceNT".equals(key) && factOper != null) {
      factOper.prc112BalanceNT = new BigDecimal(value);
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
      closeBracketList.add(() -> factOper.operDate = readDate());
      return;
    }

    // read AccMove
    if ("accCorr".equals(key) && accMove != null) {
      accMove.accCorr = value;
      return;
    }
    if ("accMoveId".equals(key) && accMove != null) {
      accMove.accMoveId = value;
      return;
    }
    if ("accNum".equals(key) && accMove != null) {
      accMove.accNum = value;
      return;
    }
    if ("accType".equals(key) && accMove != null) {
      accMove.accType = value;
      return;
    }
    if ("closeBalance".equals(key) && accMove != null) {
      accMove.closeBalance = new BigDecimal(value);
      return;
    }
    if ("closeBalanceNT".equals(key) && accMove != null) {
      accMove.closeBalanceNT = new BigDecimal(value);
      return;
    }
    if ("contractId".equals(key) && accMove != null) {
      accMove.contractId = value;
      return;
    }
    if ("openBalance".equals(key) && accMove != null) {
      accMove.openBalance = new BigDecimal(value);
      return;
    }
    if ("openBalanceNT".equals(key) && accMove != null) {
      accMove.openBalanceNT = new BigDecimal(value);
      return;
    }
    if ("operDate".equals(key) && accMove != null) {
      closeBracketList.add(() -> accMove.operDate = readDate());
      return;
    }
    if ("turnCred".equals(key) && accMove != null) {
      accMove.turnCred = new BigDecimal(value);
      return;
    }
    if ("turnCredNT".equals(key) && accMove != null) {
      accMove.turnCredNT = new BigDecimal(value);
      return;
    }
    if ("turnDebt".equals(key) && accMove != null) {
      accMove.turnDebt = new BigDecimal(value);
      return;
    }
    if ("turnDebtNT".equals(key) && accMove != null) {
      accMove.turnDebtNT = new BigDecimal(value);
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
      "no, contractId, operDate, overdueCred, overdueCredNT, overduePrc, overduePrcNT, payCred, payCredNT, payPrc, " +
      " payPrc112, payPrc112NT, payPrcNT, penyCalcBalance, penyCalcBalanceNT, penyPay, penyPayNT, prc112Balance, " +
      " prc112BalanceNT, valuta" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");

    accMovePS = connection.prepareStatement("insert into acc_move (" +
      "no, accCorr, accMoveId, accNum, accType, closeBalance, closeBalanceNT, contractId, openBalance, openBalanceNT, " +
      " operDate, turnCred, turnCredNT, turnDebt, turnDebtNT, valuta" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");
  }

  @Override
  protected void createTables() throws SQLException {
    DbAccess.createTable(connection, "create table fact_oper (" +
      "   no bigint primary key," +
      "   contractId varchar(20)," +
      "   operDate date," +
      "   overdueCred decimal," +
      "   overdueCredNT decimal," +
      "   overduePrc decimal," +
      "   overduePrcNT decimal," +
      "   payCred decimal," +
      "   payCredNT decimal," +
      "   payPrc decimal," +
      "   payPrc112 decimal," +
      "   payPrc112NT decimal," +
      "   payPrcNT decimal," +
      "   penyCalcBalance decimal," +
      "   penyCalcBalanceNT decimal," +
      "   penyPay decimal," +
      "   penyPayNT decimal," +
      "   prc112Balance decimal," +
      "   prc112BalanceNT decimal," +
      "   valuta varchar(20)" +
      ")");

    DbAccess.createTable(connection, "create table acc_move (" +
      "   no bigint primary key," +
      "   accCorr varchar(30)," +
      "   accMoveId varchar(30)," +
      "   accNum varchar(30)," +
      "   accType varchar(30)," +
      "   closeBalance decimal," +
      "   closeBalanceNT decimal," +
      "   contractId varchar(20)," +
      "   openBalance decimal," +
      "   openBalanceNT decimal," +
      "   operDate Date," +
      "   turnCred decimal," +
      "   turnCredNT decimal," +
      "   turnDebt decimal," +
      "   turnDebtNT decimal," +
      "   valuta varchar(20)" +
      ")");
  }

  private void addFactOperToBatch() throws SQLException {
    if (factOper == null) return;

    int ind = 1;
    factOperPS.setLong(ind++, factOperNo++);
    factOperPS.setString(ind++, factOper.contractId);
    factOperPS.setObject(ind++, toDate(factOper.operDate));
    factOperPS.setBigDecimal(ind++, factOper.overdueCred);
    factOperPS.setBigDecimal(ind++, factOper.overdueCredNT);
    factOperPS.setBigDecimal(ind++, factOper.overduePrc);
    factOperPS.setBigDecimal(ind++, factOper.overdueCredNT);
    factOperPS.setBigDecimal(ind++, factOper.payCred);
    factOperPS.setBigDecimal(ind++, factOper.payCredNT);
    factOperPS.setBigDecimal(ind++, factOper.payPrc);
    factOperPS.setBigDecimal(ind++, factOper.payPrc112);
    factOperPS.setBigDecimal(ind++, factOper.payPrc112NT);
    factOperPS.setBigDecimal(ind++, factOper.payPrcNT);
    factOperPS.setBigDecimal(ind++, factOper.penyCalcBalance);
    factOperPS.setBigDecimal(ind++, factOper.penyCalcBalanceNT);
    factOperPS.setBigDecimal(ind++, factOper.penyPay);
    factOperPS.setBigDecimal(ind++, factOper.penyPayNT);
    factOperPS.setBigDecimal(ind++, factOper.prc112Balance);
    factOperPS.setBigDecimal(ind++, factOper.prc112BalanceNT);
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
    accMovePS.setString(ind++, accMove.accCorr);
    accMovePS.setString(ind++, accMove.accMoveId);
    accMovePS.setString(ind++, accMove.accNum);
    accMovePS.setString(ind++, accMove.accType);
    accMovePS.setBigDecimal(ind++, accMove.closeBalance);
    accMovePS.setBigDecimal(ind++, accMove.closeBalanceNT);
    accMovePS.setString(ind++, accMove.contractId);
    accMovePS.setBigDecimal(ind++, accMove.openBalance);
    accMovePS.setBigDecimal(ind++, accMove.openBalanceNT);
    accMovePS.setObject(ind++, toDate(accMove.operDate));
    accMovePS.setBigDecimal(ind++, accMove.turnCred);
    accMovePS.setBigDecimal(ind++, accMove.turnCredNT);
    accMovePS.setBigDecimal(ind++, accMove.turnDebt);
    accMovePS.setBigDecimal(ind++, accMove.turnDebtNT);
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
