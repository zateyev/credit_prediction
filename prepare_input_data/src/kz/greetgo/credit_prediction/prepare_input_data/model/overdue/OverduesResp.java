package kz.greetgo.credit_prediction.prepare_input_data.model.overdue;

import java.math.BigDecimal;
import java.util.Date;

public class OverduesResp {

  public BigDecimal activeSumma;
  public BigDecimal activeSummaNT;
  public BigDecimal calcPenyDebt;
  public BigDecimal calcPenyDebtNT;
  public String commentFromCFT;
  public String contractId;
  public String credExpert;
  public String credManagerADUser;
  public String credManagerDepCode;
  public Date dateProlongation;
  public BigDecimal debtAll;
  public BigDecimal debtAllNT;
  public BigDecimal debtOnDate;
  public BigDecimal debtOnDateNT;

  public Date lastPayDate;

  public int overdueDay;
  public BigDecimal overduePrcDebt;
  public BigDecimal overduePrcDebtNT;
  public BigDecimal planDebtOnDate;
  public BigDecimal planDebtOnDateNT;
  public BigDecimal planPrcDebt;
  public BigDecimal planPrcDebtNT;

}
