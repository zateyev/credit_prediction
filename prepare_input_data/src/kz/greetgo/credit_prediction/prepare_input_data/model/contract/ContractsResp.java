package kz.greetgo.credit_prediction.prepare_input_data.model.contract;

public class ContractsResp {
  public Client client;
  public Collateral collateral;

  public String collateralAdd;
  public String collateralVal;
  public String conPers;

  public Credit credit;
  public Phone phone;

  public PlanOper planOper;

  @Override
  public String toString() {
    return "ContractsResp{" +
      "client=" + client +
      ", collateral=" + collateral +
      ", collateralAdd='" + collateralAdd + '\'' +
      ", collateralVal='" + collateralVal + '\'' +
      ", conPers='" + conPers + '\'' +
      ", credit=" + credit +
      ", phone=" + phone +
      ", planOper=" + planOper +
      '}';
  }
}
