package kz.greetgo.credit_prediction.prepare_input_data.parser;

import java.sql.SQLException;

public interface CloseBracket {
  void close() throws SQLException;
}
