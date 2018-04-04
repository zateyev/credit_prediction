package kz.greetgo.credit_prediction.prepare_input_data.model.transaction;

import java.math.BigDecimal;
import java.util.Date;

public class FactOper {

  public String contract_id;
  public Date oper_date;

  public BigDecimal overdue_cred;
  public BigDecimal overdue_cred_nt;
  public BigDecimal overdue_prc;
  public BigDecimal overdue_prc_nt;
  public BigDecimal pay_cred;
  public BigDecimal pay_cred_nt;
  public BigDecimal pay_prc;
  public BigDecimal pay_prc_112;
  public BigDecimal pay_prc_112_nt;
  public BigDecimal pay_prc_nt;
  public BigDecimal peny_calc_balance;
  public BigDecimal peny_calc_balance_nt;
  public BigDecimal peny_pay;
  public BigDecimal peny_pay_nt;
  public BigDecimal prc_112_balance;
  public BigDecimal prc_112_balance_nt;
  public String valuta;

}
