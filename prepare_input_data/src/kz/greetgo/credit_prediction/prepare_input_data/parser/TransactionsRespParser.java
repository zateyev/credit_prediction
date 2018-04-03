package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.transaction.FactOper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TransactionsRespParser implements AutoCloseable {
  private final Connection connection;
  private final int maxBatchSize;

  private FactOper factOper;
  private PreparedStatement factOperPS;
  private int factOperBatchSize = 0;
  private long factOperNo = 1;
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
    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.FactOper@")) {
      factOper = new FactOper();
      closeBracketList.add(this::addFactOperToBatch);
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
    if ("operDate".equals(key) && factOper != null) {
      closeBracketList.add(() -> factOper.operDate = readDate());
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
    if (factOperBatchSize > 0) {
      factOperPS.executeBatch();
      factOperBatchSize = 0;
    }
    connection.commit();
  }

  public TransactionsRespParser(Connection connection, int maxBatchSize) throws SQLException {
    this.connection = connection;
    this.maxBatchSize = maxBatchSize;

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

    connection.setAutoCommit(false);

    factOperPS = connection.prepareStatement("insert into fact_oper (" +
      "no, contractId, operDate, overdueCred, overdueCredNT, overduePrc, overduePrcNT, payCred, payCredNT, payPrc, " +
      " payPrc112, payPrc112NT, payPrcNT, penyCalcBalance, penyCalcBalanceNT, penyPay, penyPayNT, prc112Balance, " +
      " prc112BalanceNT, valuta" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
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

  private static java.sql.Date toDate(Date javaDate) {
    return javaDate == null ? null : new java.sql.Date(javaDate.getTime());
  }

  @Override
  public void close() throws Exception {
    if (factOperPS != null) {
      factOperPS.close();
      factOperPS = null;
    }
    connection.setAutoCommit(true);
  }
}
