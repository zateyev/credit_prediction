package kz.greetgo.credit_prediction.prepare_input_data.model.contract;

public class ContractsResp {
  public Client client;
  public Collateral collateral;

  public String collateral_add;
  public String collateral_val;
  public String con_pers;

  public Credit credit;
  public Phone phone;

  public PlanOper plan_oper;

  @Override
  public String toString() {
    return "ContractsResp{" +
      "client=" + client +
      ", collateral=" + collateral +
      ", collateral_add='" + collateral_add + '\'' +
      ", collateral_val='" + collateral_val + '\'' +
      ", con_pers='" + con_pers + '\'' +
      ", credit=" + credit +
      ", phone=" + phone +
      ", plan_oper=" + plan_oper +
      '}';
  }
}
