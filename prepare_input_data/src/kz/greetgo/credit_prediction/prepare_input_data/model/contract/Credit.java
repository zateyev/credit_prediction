package kz.greetgo.credit_prediction.prepare_input_data.model.contract;

import java.math.BigDecimal;
import java.util.Date;

public class Credit {

  public String contract_id;
  public String client_id;
  public String branch;
  public String branch_code;
  public String contract_manager;
  public String contract_manager_ad_user;
  public String cred_line_id;
  public String depart_code;
  public String depart_name;
  public BigDecimal dog_summa;
  public BigDecimal dog_summa_nt;
  public int grace_period;
  public String group_conv_num;
  public String kind_credit;
  public String method_calc_prc;
  public String name_group_client;
  public String num_dog;
  public String num_dog_cred_line;
  public String pod_sector_cred;
  public BigDecimal prc_rate;
  public String pre_payment_acc;
  public String product;
  public BigDecimal rate_admin_prc;
  public String sector_cred;
  public String code_group_client;
  public String contract_manager_dep_code;
  public int stupen_cred;
  public BigDecimal sum_admin_prc;
  public BigDecimal sum_admin_prc_nt;
  public BigDecimal sum_cred_line;

  public String valuta;

  public Date date_begin;
  public Date date_end;

  public Date date_open;
}
