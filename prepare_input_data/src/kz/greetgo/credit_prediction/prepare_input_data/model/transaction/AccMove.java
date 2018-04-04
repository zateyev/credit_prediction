package kz.greetgo.credit_prediction.prepare_input_data.model.transaction;

import java.math.BigDecimal;
import java.util.Date;

public class AccMove {

  public String acc_corr;
  public String acc_move_id;
  public String acc_num;
  public String acc_type;
  public BigDecimal close_balance;
  public BigDecimal close_balance_nt;
  public String contract_id;
  public BigDecimal open_balance;
  public BigDecimal open_balance_nt;
  public Date oper_date;
  public BigDecimal turn_cred;
  public BigDecimal turn_cred_nt;
  public BigDecimal turn_debt;
  public BigDecimal turn_debt_nt;
  public String valuta;

}
