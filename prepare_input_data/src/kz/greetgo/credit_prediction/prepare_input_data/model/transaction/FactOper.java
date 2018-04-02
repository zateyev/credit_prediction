package kz.greetgo.credit_prediction.prepare_input_data.model.transaction;

import java.math.BigDecimal;
import java.util.Date;

public class FactOper {

  public String contractId;
  public Date operDate;

  public BigDecimal overdueCred;
  public BigDecimal overdueCredNT;
  public BigDecimal overduePrc;
  public BigDecimal overduePrcNT;
  public BigDecimal payCred;
  public BigDecimal payCredNT;
  public BigDecimal payPrc;
  public BigDecimal payPrc112;
  public BigDecimal payPrc112NT;
  public BigDecimal payPrcNT;
  public BigDecimal penyCalcBalance;
  public BigDecimal penyCalcBalanceNT;
  public BigDecimal penyPay;
  public BigDecimal penyPayNT;
  public BigDecimal prc112Balance;
  public BigDecimal prc112BalanceNT;
  public String valuta;

}
