package kz.greetgo.credit_prediction.prepare_input_data.model.transaction;

import java.math.BigDecimal;
import java.util.Date;

public class AccMove {

  public String accCorr;
  public String accMoveId;
  public String accNum;
  public String accType;
  public BigDecimal closeBalance;
  public BigDecimal closeBalanceNT;
  public String contractId;
  public BigDecimal openBalance;
  public BigDecimal openBalanceNT;
  public Date operDate;
  public BigDecimal turnCred;
  public BigDecimal turnCredNT;
  public BigDecimal turnDebt;
  public BigDecimal turnDebtNT;
  public String valuta;

}
