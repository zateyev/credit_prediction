package kz.greetgo.credit_prediction.prepare_input_data.util;

import kz.greetgo.db.AbstractJdbcWithDataSource;
import kz.greetgo.db.TransactionManager;

import javax.sql.DataSource;

public class JdbcCreditPrediction extends AbstractJdbcWithDataSource {
  private final DataSource dataSource;
  private final TransactionManager transactionManager;

  public JdbcCreditPrediction(DataSource dataSource, TransactionManager transactionManager) {
    this.dataSource = dataSource;
    this.transactionManager = transactionManager;
  }

  @Override
  protected DataSource getDataSource() {
    return dataSource;
  }

  @Override
  protected TransactionManager getTransactionManager() {
    return transactionManager;
  }
}
