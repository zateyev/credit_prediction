package kz.greetgo.credit_prediction.prepare_input_data.client_to_json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SelectorAsJson implements AutoCloseable {

  public Connection connection;

  private final AtomicBoolean working;
  private final AtomicBoolean showStatus;

  public SelectorAsJson(Connection connection) {
    this.connection = connection;

    working = new AtomicBoolean(true);
    showStatus = new AtomicBoolean(false);
    final Thread see = new Thread(() -> {

      while (working.get()) {

        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          break;
        }

        showStatus.set(true);

      }

    });
    see.start();
  }

  @SuppressWarnings("SameParameterValue")
  public void createClientJsonFiles(String pathToSave) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
    List<String> ret = new ArrayList<>();
    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT to_json(t) FROM \n" +
      " (select no, client_id, firstname, \n" +
      " \n" +
      "  (select array_to_json(array_agg(r)) FROM \n" +
      "   (select *, \n" +
      "    (select array_to_json(array_agg(po)) FROM (select * from plan_oper_tmp where contract_id = credit_tmp.contract_id) po) as plan_oper,\n" +
      "    (select array_to_json(array_agg(ov)) FROM (select * from overdue where contract_id = credit_tmp.contract_id) ov) as overdue,\n" +
      "    (select array_to_json(array_agg(fo)) FROM (select * from fact_oper where contract_id = credit_tmp.contract_id) fo) as fact_oper,\n" +
      "    (select array_to_json(array_agg(am)) FROM (select * from acc_move where contract_id = credit_tmp.contract_id) am) as acc_move,\n" +
      "    (select array_to_json(array_agg(co)) FROM (select * from collateral where contract_id = credit_tmp.contract_id) co) as acc_move\n" +
      "   from credit_tmp where client_id = client_tmp.client_id) \n" +
      "  r) as credit,\n" +
      "  \n" +
      "  (select array_to_json(array_agg(ph)) FROM (select * from phone_tmp where client_id = client_tmp.client_id) ph) as phone\n" +
      " from client_tmp) t")) {
//      " from client_tmp LIMIT 15) t")) {

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        int i = 0;
        PrintWriter writer;
        File file;

        while (resultSet.next()) {
          file = new File(pathToSave + "/client-" + i + ".json_row.txt");
          //noinspection ResultOfMethodCallIgnored
          file.getParentFile().mkdirs();
          writer = new PrintWriter(file, "UTF-8");
          String json = resultSet.getString(1);
          writer.println(json);
          writer.close();

          if (showStatus.get()) {
            showStatus.set(false);
            System.out.println("Creating files");
          }

          i++;
        }

        System.out.println("Created " + i + " files");
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (connection != null) {
      connection.close();
      connection = null;
    }
    working.set(false);
  }
}
