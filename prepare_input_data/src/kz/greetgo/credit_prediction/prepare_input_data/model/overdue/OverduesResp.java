package kz.greetgo.credit_prediction.prepare_input_data.model.overdue;

import java.math.BigDecimal;
import java.util.Date;

public class OverduesResp {

  public BigDecimal active_summa;
  public BigDecimal active_summa_nt;
  public BigDecimal calc_peny_debt;
  public BigDecimal calc_peny_debt_nt;
  public String comment_from_cft;
  public String contract_id;
  public String cred_expert;
  public String cred_manager_ad_user;
  public String cred_manager_dep_code;
  public Date date_prolongation;
  public BigDecimal debt_all;
  public BigDecimal debt_all_nt;
  public BigDecimal debt_on_date;
  public BigDecimal debt_on_date_nt;

  public Date last_pay_date;

  public int overdue_day;
  public BigDecimal overdue_prc_debt;
  public BigDecimal overdue_prc_debt_nt;
  public BigDecimal plan_debt_on_date;
  public BigDecimal plan_debt_on_date_nt;
  public BigDecimal plan_prc_debt;
  public BigDecimal plan_prc_debt_nt;

}
