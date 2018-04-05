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
    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT to_json(t) FROM \n" +
      " (select client_id, date_birth, firstname, surname, patronymic, inn, num_seria_passport, sex, type, fact_address, reg_address, type_passport, who_issue_passport, \n" +
      "         phys_work_place, jur_name, jur_registr, okved, phys_casta, workplace_spouse, \n" +
      " \n" +
      "  (select array_to_json(array_agg(r)) FROM \n" +
      "   (select contract_id, branch, branch_code, contract_manager, contract_manager_ad_user, cred_line_id, depart_code, depart_name, \n" +
      "           dog_summa, dog_summa_nt, grace_period, group_conv_num, kind_credit, method_calc_prc, name_group_client, num_dog, num_dog_cred_line, \n" +
      "           pod_sector_cred, prc_rate, pre_payment_acc, product, rate_admin_prc, sector_cred, code_group_client, contract_manager_dep_code, stupen_cred, \n" +
      "           sum_admin_prc, sum_admin_prc_nt, sum_cred_line, valuta, date_begin, date_end, date_open, \n" +
      "           \n" +
      "    (select array_to_json(array_agg(po)) FROM (select \n" +
      "        cred_summa, debt_cred_balance, dog_summa, month_summa, prc_summa, valuta, plan_date\n" +
      "     from plan_oper_tmp where contract_id = credit_tmp.contract_id) po) as plan_oper,\n" +
      "    \n" +
      "    (select array_to_json(array_agg(ov)) FROM (select * from overdue where contract_id = credit_tmp.contract_id) ov) as overdue,\n" +
      "    (select array_to_json(array_agg(fo)) FROM (select * from fact_oper where contract_id = credit_tmp.contract_id) fo) as fact_oper,\n" +
      "    (select array_to_json(array_agg(am)) FROM (select * from acc_move where contract_id = credit_tmp.contract_id) am) as acc_move,\n" +
      "    (select array_to_json(array_agg(co)) FROM (select * from collateral where contract_id = credit_tmp.contract_id) co) as collateral\n" +
      "   from credit_tmp where client_id = client_tmp.client_id) \n" +
      "  r) as credit,\n" +
      "  \n" +
      "  (select array_to_json(array_agg(ph)) FROM (select \n" +
      "      phone_id, phone_num_status, phone_num_type, phone_numb\n" +
      "   from phone_tmp where client_id = client_tmp.client_id) ph) as phone\n" +
      " from client_tmp ORDER BY no) t")) {
//      " from client_tmp LIMIT 15) t")) {

      System.out.println("Selecting clients as json...");
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        System.out.println("Started creating of files...");
        int i = 0;
        PrintWriter writer;
        File file;

        while (resultSet.next()) {
          file = new File(pathToSave + "/client-" + i + ".json");
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
