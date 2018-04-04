package kz.greetgo.credit_prediction.prepare_input_data.model.contract;

import java.util.Date;

public class Client {
  public int bic;
  private int bin;

  public String client_id;
  public String inn;
  public String firstname;
  public String surname;
  public String patronymic;
  public String sex;
  public Date date_birth;
  public String type;

  public String fact_address;
  public String reg_address;

  public String num_seria_passport;

  public String phys_work_place;
  public String jur_name;
  public String jur_registr;
  public String okved;
  public String phys_casta;
  public String workplace_spouse;

  public Date date_issue_passport;
  public String type_passport;
  public String who_issue_passport;

  @Override
  public String toString() {
    return "Client{" +
      "bic=" + bic +
      ", bin=" + bin +
      ", client_id='" + client_id + '\'' +
      ", inn='" + inn + '\'' +
      ", firstname='" + firstname + '\'' +
      ", surname='" + surname + '\'' +
      ", patronymic='" + patronymic + '\'' +
      ", sex='" + sex + '\'' +
      ", date_birth=" + date_birth +
      ", type='" + type + '\'' +
      ", fact_address='" + fact_address + '\'' +
      ", reg_address='" + reg_address + '\'' +
      ", jur_name='" + jur_name + '\'' +
      ", jur_registr='" + jur_registr + '\'' +
      ", num_seria_passport='" + num_seria_passport + '\'' +
      ", okved='" + okved + '\'' +
      ", phys_casta='" + phys_casta + '\'' +
      ", phys_work_place='" + phys_work_place + '\'' +
      ", workplace_spouse='" + workplace_spouse + '\'' +
      ", date_issue_passport=" + date_issue_passport +
      ", type_passport='" + type_passport + '\'' +
      ", who_issue_passport='" + who_issue_passport + '\'' +
      '}';
  }
}
