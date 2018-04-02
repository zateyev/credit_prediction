package kz.greetgo.credit_prediction.prepare_input_data.model.contract;

import java.util.Date;

public class Client {
  public int bic;
  private int bin;
  public String clientId;
  public String inn;

  public String firstname;
  public String surname;
  public String patronymic;
  public String sex;
  public Date dateBirth;
  public String type;

  public String factAddress;
  public String regAddress;

  public String jurName;
  public String jurRegistr;
  public String numSeriaPassport;
  public String okved;
  public String physCasta;
  public String physWorkPlace;
  public String workplaceSpouse;

  public Date dateIssuePassport;
  public String typePassport;
  public String whoIssuePassport;
}
