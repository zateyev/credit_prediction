package kz.greetgo.credit_prediction.prepare_input_data.client_to_json;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Select {
  public Connection connection;

  public void createFiles() throws SQLException {
    //language=PostgreSQL
    exec("" +
      "SELECT array_to_json(array_agg(t)) FROM \n" +
      " (select no, clientid, firstname, \n" +
      " \n" +
      "  (select array_to_json(array_agg(r)) FROM \n" +
      "   (select no, branch, \n" +
      "    (select array_to_json(array_agg(po)) FROM (select credsumma, dogsumma from plan_oper_tmp where contractid = credit_tmp.contractid) po) as plan_oper,\n" +
      "    (select array_to_json(array_agg(ov)) FROM (select activesumma, activesummant from overdue where contractid = credit_tmp.contractid) ov) as overdue,\n" +
      "    (select array_to_json(array_agg(fo)) FROM (select operdate, overduecred from fact_oper where contractid = credit_tmp.contractid) fo) as fact_oper,\n" +
      "    (select array_to_json(array_agg(am)) FROM (select closebalance, openbalance from acc_move where contractid = credit_tmp.contractid) am) as acc_move\n" +
      "   from credit_tmp where clientid = client_tmp.clientid) \n" +
      "  r) as credit,\n" +
      "  \n" +
      "  \n" +
      "  (select array_to_json(array_agg(ph)) FROM (select no, phonenumb from phone_tmp where clientid = client_tmp.clientid) ph) as phone\n" +
      " from client_tmp limit 2) t" +
      "");
  }

  private void exec(String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
    }
  }

  public static void main(String[] args) throws Exception {
    Select select = new Select();
    try (Connection connection = DbAccess.createConnection()) {
      select.connection = connection;
      List<String> clientsAsJson = select.getClientsAsJson();
//      select.createFiles();
    }

  }

  private List<String> getClientsAsJson() throws SQLException {
    List<String> ret = new ArrayList<>();
    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT to_json(t) FROM \n" +
      " (select no, clientid, firstname, \n" +
      " \n" +
      "  (select array_to_json(array_agg(r)) FROM \n" +
      "   (select *, \n" +
      "    (select array_to_json(array_agg(po)) FROM (select * from plan_oper_tmp where contractid = credit_tmp.contractid) po) as plan_oper,\n" +
      "    (select array_to_json(array_agg(ov)) FROM (select * from overdue where contractid = credit_tmp.contractid) ov) as overdue,\n" +
      "    (select array_to_json(array_agg(fo)) FROM (select * from fact_oper where contractid = credit_tmp.contractid) fo) as fact_oper,\n" +
      "    (select array_to_json(array_agg(am)) FROM (select * from acc_move where contractid = credit_tmp.contractid) am) as acc_move\n" +
      "   from credit_tmp where clientid = client_tmp.clientid) \n" +
      "  r) as credit,\n" +
      "  \n" +
      "  (select array_to_json(array_agg(ph)) FROM (select * from phone_tmp where clientid = client_tmp.clientid) ph) as phone\n" +
      " from client_tmp limit 2) t")) {

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          String string = resultSet.getString(1);
          System.out.println(string + ", end of first");

        }
      }
    }
    return ret;
  }
}
