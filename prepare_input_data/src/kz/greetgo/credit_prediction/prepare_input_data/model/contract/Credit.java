package kz.greetgo.credit_prediction.prepare_input_data.model.contract;

import java.math.BigDecimal;
import java.util.Date;

public class Credit {

  public String branch;
  public String branchCode;
  public String clientId;
  public String codeGroupClient;
  public String contractId;
  public String contractManager;
  public String contractManagerADUser;
  public String contractManagerDepCode;
  public String credLineId;

  public Date dateBegin;
  public Date dateEnd;
  public Date dateOpen;

  public String departCode;
  public String departName;
  public BigDecimal dogSumma;
  public BigDecimal dogSummaNT;
  public int gracePeriod;
  public String groupConvNum;
  public String kindCredit;
  public String methodCalcPrc;
  public String nameGroupClient;
  public String numDog;
  public String numDogCredLine;
  public String podSectorCred;
  public BigDecimal prcRate;
  public String prePaymentAcc;
  public String product;
  public BigDecimal rateAdminPrc;
  public String sectorCred;
  public int stupenCred;
  public BigDecimal sumAdminPrc;
  public BigDecimal sumAdminPrcNT;
  public BigDecimal sumCredLine;
  public String valuta;
}
