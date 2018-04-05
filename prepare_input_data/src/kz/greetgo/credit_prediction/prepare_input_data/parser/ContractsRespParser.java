package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import kz.greetgo.credit_prediction.prepare_input_data.model.contract.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ContractsRespParser extends ParserAbstract {

  private ContractsResp contractsResp = null;
  private Client client = null;
  private Credit credit = null;
  private Collateral collateral = null;
  private Phone phone = null;
  private PlanOper planOper = null;
  private long clientNo = 1;
  private long creditNo = 1;
  private long collateralNo = 1;
  private long phoneNo = 1;
  private long planOperNo = 1;

  PreparedStatement clientPS;
  PreparedStatement creditPS;
  PreparedStatement collateralPS;
  PreparedStatement phonePS;
  PreparedStatement planOperPS;

  public ContractsRespParser(Connection connection, int maxBatchSize) throws SQLException {
    super(connection, maxBatchSize);

    createTables();

    connection.setAutoCommit(false);

    clientPS = connection.prepareStatement("insert into client_tmp (" +
      "no, client_id, date_birth, firstname, surname, patronymic, inn, num_seria_passport, sex, type, fact_address, reg_address, " +
      "type_passport, who_issue_passport, phys_work_place, jur_name, jur_registr, okved, phys_casta, workplace_spouse" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");

    creditPS = connection.prepareStatement("insert into credit_tmp (" +
      "no, contract_id, client_id, branch, branch_code, contract_manager, contract_manager_ad_user, cred_line_id, depart_code, depart_name, " +
      "dog_summa, dog_summa_nt, grace_period, group_conv_num, kind_credit, method_calc_prc, name_group_client, num_dog, num_dog_cred_line, " +
      "pod_sector_cred, prc_rate, pre_payment_acc, product, rate_admin_prc, sector_cred, code_group_client, contract_manager_dep_code, stupen_cred, " +
      "sum_admin_prc, sum_admin_prc_nt, sum_cred_line, valuta, date_begin, date_end, date_open" + //35
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
      ")");

    collateralPS = connection.prepareStatement("insert into collateral (" +
      "no, address, collateral_id, collateral_type, contract_id, description, insurance_company, mortgagor, " +
      " percentage, summa, summa_nt" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?,?,?" +
      ")");

    phonePS = connection.prepareStatement("insert into phone_tmp (" +
      "no, client_id, phone_id, phone_num_status, phone_num_type, phone_numb" +
      ") values (" +
      " ?,?,?,?,?,?" +
      ")");

    planOperPS = connection.prepareStatement("insert into plan_oper_tmp (" +
      "no, contract_id, cred_summa, debt_cred_balance, dog_summa, month_summa, prc_summa, valuta, plan_date" +
      ") values (" +
      " ?,?,?,?,?,?,?,?,?" +
      ")");
  }

  @Override
  protected void createTables() throws SQLException {
    DbAccess.createTable(connection, "create table client_tmp (" +
      "  no         bigint primary key," +
      "  status int not null default 0," +
      "  client_id   varchar(20)," +
      "  date_birth  date," +
      "  firstname  varchar(300)," +
      "  surname    varchar(300)," +
      "  patronymic varchar(300)," +
      "  inn varchar(50)," +
      "  num_seria_passport varchar(50)," +
      "  sex varchar(20)," +
      "  type varchar(30)," +
      "  fact_address varchar(300)," +
      "  reg_address varchar(300)," +
      "  type_passport varchar(30)," +
      "  who_issue_passport varchar(300)," +
      "  phys_work_place varchar(300)," +
      "  jur_name varchar(300)," +
      "  jur_registr varchar(300)," +
      "  okved varchar(300)," +
      "  phys_casta varchar(300)," +
      "  workplace_spouse varchar(300)" +
      ")");
    DbAccess.createTable(connection, "create table credit_tmp (" +
      "  no         bigint primary key," +
      "  status int not null default 0," +
      "  contract_id   varchar(20)," +
      "  client_id   varchar(20)," +
      "  branch  varchar(300)," +
      "  branch_code  varchar(300)," +
      "  contract_manager    varchar(300)," +
      "  contract_manager_ad_user varchar(300)," +
      "  cred_line_id varchar(300)," +
      "  depart_code varchar(300)," +
      "  depart_name varchar(300)," +
      "  dog_summa decimal," +
      "  dog_summa_nt decimal," +
      "  grace_period int," +
      "  group_conv_num varchar(300)," +
      "  kind_credit varchar(300)," +
      "  method_calc_prc varchar(300)," +
      "  name_group_client varchar(300)," +
      "  num_dog varchar(50)," +
      "  num_dog_cred_line varchar(50)," +
      "  pod_sector_cred varchar(300)," +
      "  prc_rate decimal," +
      "  pre_payment_acc varchar(300)," +
      "  product varchar(300)," +
      "  rate_admin_prc decimal," +
      "  sector_cred varchar(300)," +
      "  code_group_client varchar(300)," +
      "  contract_manager_dep_code varchar(300)," +
      "  stupen_cred int," +
      "  sum_admin_prc decimal," +
      "  sum_admin_prc_nt decimal," +
      "  sum_cred_line decimal," +
      "  valuta varchar(20)," +
      "  date_begin  date," +
      "  date_end  date," +
      "  date_open  date" +
      ")");
    DbAccess.createTable(connection, "create table collateral (" +
      "  no             bigint primary key," +
      "  status int not null default 0," +
      "  address varchar(300)," +
      "  collateral_id varchar(300)," +
      "  collateral_type varchar(300)," +
      "  contract_id varchar(300)," +
      "  description varchar(300)," +
      "  insurance_company varchar(300)," +
      "  mortgagor varchar(300)," +
      "  percentage decimal," +
      "  summa decimal," +
      "  summa_nt decimal" +
      ")");
    DbAccess.createTable(connection, "create table phone_tmp (" +
      "  no             bigint primary key," +
      "  status int not null default 0," +
      "  client_id       varchar(20)," +
      "  phone_id        varchar(20)," +
      "  phone_num_status varchar(50)," +
      "  phone_num_type   varchar(50)," +
      "  phone_numb      varchar(50)" +
      ")");
    DbAccess.createTable(connection, "create table plan_oper_tmp (" +
      "  no              bigint primary key," +
      "  contract_id      varchar(20)," +
      "  cred_summa       decimal," +
      "  debt_cred_balance decimal," +
      "  dog_summa        decimal," +
      "  month_summa      decimal," +
      "  prc_summa        decimal," +
      "  valuta          varchar(20)," +
      "  plan_date        date" +
      ")");
  }

  int clientBatchSize = 0;
  int creditBatchSize = 0;
  int collateralBatchSize = 0;
  int phoneBatchSize = 0;
  int planOperBatchSize = 0;

  private void goContractsResp() throws SQLException {
    if (contractsResp == null) return;

    int ind = 1;
    clientPS.setLong(ind++, clientNo++);
    clientPS.setString(ind++, client.client_id);
    clientPS.setObject(ind++, toDate(client.date_birth));
    clientPS.setString(ind++, client.firstname);
    clientPS.setString(ind++, client.surname);
    clientPS.setString(ind++, client.patronymic);
    clientPS.setString(ind++, client.inn);
    clientPS.setString(ind++, client.num_seria_passport);
    clientPS.setString(ind++, client.sex);
    clientPS.setString(ind++, client.type);
    clientPS.setString(ind++, client.fact_address);
    clientPS.setString(ind++, client.reg_address);
    clientPS.setString(ind++, client.type_passport);
    clientPS.setString(ind++, client.who_issue_passport);
    clientPS.setString(ind++, client.phys_work_place);
    clientPS.setString(ind++, client.jur_name);
    clientPS.setString(ind++, client.jur_registr);
    clientPS.setString(ind++, client.okved);
    clientPS.setString(ind++, client.phys_casta);
    clientPS.setString(ind, client.workplace_spouse);
    clientPS.addBatch();
    clientBatchSize++;

    if (maxBatchSize <= clientBatchSize) {
      clientPS.executeBatch();
      connection.commit();
      clientBatchSize = 0;
    }

//    System.out.println(contractsResp);
  }

  @Override
  protected void finish() {
    try {
      if (clientBatchSize > 0) {
        clientPS.executeBatch();
        clientBatchSize = 0;
      }
      if (creditBatchSize > 0) {
        try {

          creditPS.executeBatch();
        }catch (SQLException e) {
          throw new RuntimeException(e.getNextException());
        }
        creditBatchSize = 0;
      }
      if (phoneBatchSize > 0) {
        phonePS.executeBatch();
        phoneBatchSize = 0;
      }
      if (planOperBatchSize > 0) {
        planOperPS.executeBatch();
        planOperBatchSize = 0;
      }
      if (collateralBatchSize > 0) {
        collateralPS.executeBatch();
        collateralBatchSize = 0;
      }
      connection.commit();
      goContractsResp();
    } catch (SQLException e) {
      throw new RuntimeException(e.getNextException());
    }
  }

  @Override
  public void close() throws Exception {
    if (clientPS != null) {
      clientPS.close();
      clientPS = null;
    }
    if (creditPS != null) {
      creditPS.close();
      creditPS = null;
    }
    if (collateralPS != null) {
      collateralPS.close();
      collateralPS = null;
    }
    if (phonePS != null) {
      phonePS.close();
      phonePS = null;
    }
    if (planOperPS != null) {
      planOperPS.close();
      planOperPS = null;
    }
    connection.setAutoCommit(true);
  }

  @Override
  protected void readLine(String line, int lineNo) throws SQLException {
    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.ContractsResp@")) {
      goContractsResp();
      contractsResp = new ContractsResp();
      return;
    }

    if (line.trim().startsWith("client=kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Client@")) {
      client = new Client();
      contractsResp.client = client;
      return;
    }

    if (line.trim().startsWith("credit=kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Credit@")) {
      credit = new Credit();
      closeBracketList.add(this::addCreditToBatch);
      return;
    }

    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Collateral@")) {
      collateral = new Collateral();
      closeBracketList.add(this::addCollateralToBatch);
      return;
    }

    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.Phone@")) {
      phone = new Phone();
      closeBracketList.add(this::addPhoneToBatch);
      return;
    }

    if (line.trim().startsWith("kz.greetgo.collect.wsdlclient.gen.callcollectHumo.PlanOper@")) {
      planOper = new PlanOper();
      closeBracketList.add(this::addPlanOperToBatch);
      return;
    }

    {
      int eqIndex = line.indexOf('=');
      if (eqIndex > -1) {
        String key = line.substring(0, eqIndex).trim();
        String value = line.substring(eqIndex + 1).trim();
        if ("<null>".equals(value)) value = null;
        readKeyValue(key, value);
        return;
      }
    }

    if ("]".equals(line.trim())) {
      if (closeBracketList.size() > 0) closeBracketList.remove(closeBracketList.size() - 1).close();
      return;
    }
  }

  private void addCollateralToBatch() throws SQLException {
    if (collateral == null) return;

    int ind = 1;
    collateralPS.setLong(ind++, collateralNo++);
    collateralPS.setString(ind++, collateral.address);
    collateralPS.setString(ind++, collateral.collateral_id);
    collateralPS.setString(ind++, collateral.collateral_type);
    collateralPS.setString(ind++, collateral.contract_id);
    collateralPS.setString(ind++, collateral.description);
    collateralPS.setString(ind++, collateral.insurance_company);
    collateralPS.setString(ind++, collateral.mortgagor);
    collateralPS.setBigDecimal(ind++, collateral.percentage);
    collateralPS.setBigDecimal(ind++, collateral.summa);
    collateralPS.setBigDecimal(ind, collateral.summa_nt);
    collateralPS.addBatch();
    collateralBatchSize++;

    if (maxBatchSize <= collateralBatchSize) {
      collateralPS.executeBatch();
      connection.commit();
      collateralBatchSize = 0;
    }

    collateral = null;
  }

  private void addCreditToBatch() throws SQLException {
    if (credit == null) return;

    int ind = 1;
    creditPS.setLong(ind++, creditNo++);
    creditPS.setString(ind++, credit.contract_id);
    creditPS.setString(ind++, credit.client_id);
    creditPS.setString(ind++, credit.branch);
    creditPS.setString(ind++, credit.branch_code);
    creditPS.setString(ind++, credit.contract_manager);
    creditPS.setString(ind++, credit.contract_manager_ad_user);
    creditPS.setString(ind++, credit.cred_line_id);
    creditPS.setString(ind++, credit.depart_code);
    creditPS.setString(ind++, credit.depart_name);
    creditPS.setBigDecimal(ind++, credit.dog_summa);
    creditPS.setBigDecimal(ind++, credit.dog_summa_nt);
    creditPS.setInt(ind++, credit.grace_period);
    creditPS.setString(ind++, credit.group_conv_num);
    creditPS.setString(ind++, credit.kind_credit);
    creditPS.setString(ind++, credit.method_calc_prc);
    creditPS.setString(ind++, credit.name_group_client);
    creditPS.setString(ind++, credit.num_dog);
    creditPS.setString(ind++, credit.num_dog_cred_line);
    creditPS.setString(ind++, credit.pod_sector_cred);
    creditPS.setBigDecimal(ind++, credit.prc_rate);
    creditPS.setString(ind++, credit.pre_payment_acc);
    creditPS.setString(ind++, credit.product);
    creditPS.setBigDecimal(ind++, credit.rate_admin_prc);
    creditPS.setString(ind++, credit.sector_cred);
    creditPS.setString(ind++, credit.code_group_client);
    creditPS.setString(ind++, credit.contract_manager_dep_code);
    creditPS.setInt(ind++, credit.stupen_cred);
    creditPS.setBigDecimal(ind++, credit.sum_admin_prc);
    creditPS.setBigDecimal(ind++, credit.sum_admin_prc_nt);
    creditPS.setBigDecimal(ind++, credit.sum_cred_line);
    creditPS.setString(ind++, credit.valuta);
    creditPS.setObject(ind++, toDate(credit.date_begin));
    creditPS.setObject(ind++, toDate(credit.date_end));
    creditPS.setObject(ind, toDate(credit.date_open));
    creditPS.addBatch();
    creditBatchSize++;

    if (maxBatchSize <= creditBatchSize) {
      creditPS.executeBatch();
      connection.commit();
      creditBatchSize = 0;
    }

    credit = null;
  }

  private void addPlanOperToBatch() throws SQLException {
    if (planOper == null) return;

    int ind = 1;
    planOperPS.setLong(ind++, planOperNo++);
    planOperPS.setString(ind++, planOper.contract_id);
    planOperPS.setBigDecimal(ind++, planOper.cred_summa);
    planOperPS.setBigDecimal(ind++, planOper.debt_cred_balance);
    planOperPS.setBigDecimal(ind++, planOper.dog_summa);
    planOperPS.setBigDecimal(ind++, planOper.month_summa);
    planOperPS.setBigDecimal(ind++, planOper.prc_summa);
    planOperPS.setString(ind++, planOper.valuta);
    planOperPS.setObject(ind, toDate(planOper.plan_date));
    planOperPS.addBatch();
    planOperBatchSize++;

    if (maxBatchSize <= planOperBatchSize) {
      planOperPS.executeBatch();
      connection.commit();
      planOperBatchSize = 0;
    }

    planOper = null;
  }

  private void addPhoneToBatch() throws SQLException {
    if (phone == null) return;

    int ind = 1;
    phonePS.setLong(ind++, phoneNo++);
    phonePS.setString(ind++, phone.client_id);
    phonePS.setString(ind++, phone.phone_id);
    phonePS.setString(ind++, phone.phone_num_status);
    phonePS.setString(ind++, phone.phone_num_type);
    phonePS.setString(ind, phone.phone_numb);
    phonePS.addBatch();
    phoneBatchSize++;

    if (maxBatchSize <= phoneBatchSize) {
      phonePS.executeBatch();
      connection.commit();
      phoneBatchSize = 0;
    }

    phone = null;
  }


  boolean inDate = false;

  @Override
  protected void readKeyValue(String key, String value) {

    if (inDate && "year".equals(key)) {
      year = Integer.parseInt(value);
      return;
    }
    if (inDate && "month".equals(key)) {
      month = Integer.parseInt(value);
      return;
    }
    if (inDate && "day".equals(key)) {
      day = Integer.parseInt(value);
      return;
    }
    if ("dateBirth".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> client.date_birth = readDate());
      return;
    }
    if ("dateIssuePassport".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> client.date_issue_passport = readDate());
      return;
    }
    if ("dateBegin".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> credit.date_begin = readDate());
      return;
    }
    if ("dateEnd".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> credit.date_end = readDate());
      return;
    }
    if ("dateOpen".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> credit.date_open = readDate());
      return;
    }
    if ("planDate".equals(key)) {
      inDate = true;
      closeBracketList.add(() -> planOper.plan_date = readDate());
      return;
    }

    if ("surname".equals(key)) {
      client.surname = value;
      return;
    }
    if ("firstname".equals(key)) {
      client.firstname = value;
      return;
    }
    if ("patronymic".equals(key)) {
      client.patronymic = value;
      return;
    }
    if ("inn".equals(key)) {
      client.inn = value;
      return;
    }
    if ("numSeriaPassport".equals(key)) {
      client.num_seria_passport = value;
      return;
    }
    if ("sex".equals(key)) {
      client.sex = value;
      return;
    }
    if ("type".equals(key)) {
      client.type = value;
      return;
    }
    if ("factAddress".equals(key)) {
      client.fact_address = value;
      return;
    }
    if ("regAddress".equals(key)) {
      client.reg_address = value;
      return;
    }
    if ("typePassport".equals(key)) {
      client.type_passport = value;
      return;
    }
    if ("whoIssuePassport".equals(key)) {
      client.who_issue_passport = value;
      return;
    }
    if ("physWorkPlace".equals(key)) {
      client.phys_work_place = value;
      return;
    }
    if ("jurName".equals(key)) {
      client.jur_name = value;
      return;
    }
    if ("jurRegistr".equals(key)) {
      client.jur_registr = value;
      return;
    }
    if ("okved".equals(key)) {
      client.okved = value;
      return;
    }
    if ("physCasta".equals(key)) {
      client.phys_casta = value;
      return;
    }
    if ("workplaceSpouse".equals(key)) {
      client.workplace_spouse = value;
      return;
    }
    if ("clientId".equals(key)) {
      client.client_id = value;
      if (phone != null) {
        phone.client_id = value;
        return;
      }
      if (credit != null) {
        credit.client_id = value;
        return;
      }
    }


    // read credit fields
    if ("branch".equals(key) && credit != null) {
      credit.branch = value;
      return;
    }
    if ("branchCode".equals(key) && credit != null) {
      credit.branch_code = value;
      return;
    }
    if ("contractId".equals(key) && credit != null) {
      credit.contract_id = value;
      return;
    }
    if ("contractManager".equals(key) && credit != null) {
      credit.contract_manager = value;
      return;
    }
    if ("contractManagerADUser".equals(key) && credit != null) {
      credit.contract_manager_ad_user = value;
      return;
    }
    if ("credLineId".equals(key) && credit != null) {
      credit.cred_line_id = value;
      return;
    }
    if ("departCode".equals(key) && credit != null) {
      credit.depart_code = value;
      return;
    }
    if ("departName".equals(key) && credit != null) {
      credit.depart_name = value;
      return;
    }
    if ("dogSumma".equals(key) && credit != null) {
      credit.dog_summa = new BigDecimal(value);
      return;
    }
    if ("dogSummaNT".equals(key) && credit != null) {
      credit.dog_summa_nt = new BigDecimal(value);
      return;
    }
    if ("gracePeriod".equals(key) && credit != null) {
      credit.grace_period = Integer.parseInt(value);
      return;
    }
    if ("groupConvNum".equals(key) && credit != null) {
      credit.group_conv_num = value;
      return;
    }
    if ("kindCredit".equals(key) && credit != null) {
      credit.kind_credit = value;
      return;
    }
    if ("methodCalcPrc".equals(key) && credit != null) {
      credit.method_calc_prc = value;
      return;
    }
    if ("nameGroupClient".equals(key) && credit != null) {
      credit.name_group_client = value;
      return;
    }
    if ("numDog".equals(key) && credit != null) {
      credit.num_dog = value;
      return;
    }
    if ("numDogCredLine".equals(key) && credit != null) {
      credit.num_dog_cred_line = value;
      return;
    }
    if ("podSectorCred".equals(key) && credit != null) {
      credit.pod_sector_cred = value;
      return;
    }
    if ("prcRate".equals(key) && credit != null) {
      credit.prc_rate = new BigDecimal(value);
      return;
    }
    if ("prePaymentAcc".equals(key) && credit != null) {
      credit.pre_payment_acc = value;
      return;
    }
    if ("product".equals(key) && credit != null) {
      credit.product = value;
      return;
    }
    if ("rateAdminPrc".equals(key) && credit != null) {
      credit.rate_admin_prc = new BigDecimal(value);
      return;
    }
    if ("sectorCred".equals(key) && credit != null) {
      credit.sector_cred = value;
      return;
    }
    if ("codeGroupClient".equals(key) && credit != null) {
      credit.code_group_client = value;
      return;
    }
    if ("contractManagerDepCode".equals(key) && credit != null) {
      credit.contract_manager_dep_code = value;
      return;
    }
    if ("stupenCred".equals(key) && credit != null) {
      credit.stupen_cred = Integer.parseInt(value);
      return;
    }
    if ("sumAdminPrc".equals(key) && credit != null) {
      credit.sum_admin_prc = new BigDecimal(value);
      return;
    }
    if ("sumAdminPrcNT".equals(key) && credit != null) {
      credit.sum_admin_prc_nt = new BigDecimal(value);
      return;
    }
    if ("sumCredLine".equals(key) && credit != null) {
      credit.sum_cred_line = new BigDecimal(value);
      return;
    }
    if ("valuta".equals(key) && credit != null) {
      credit.valuta = value;
      return;
    }

    //read collateral fields
    if ("address".equals(key) && collateral != null) {
      collateral.address = value;
      return;
    }
    if ("collateralId".equals(key) && collateral != null) {
      collateral.collateral_id = value;
      return;
    }
    if ("collateralType".equals(key) && collateral != null) {
      collateral.collateral_type = value;
      return;
    }
    if ("contractId".equals(key) && collateral != null) {
      collateral.contract_id = value;
      return;
    }
    if ("description".equals(key) && collateral != null) {
      collateral.description = value;
      return;
    }
    if ("insuranceCompany".equals(key) && collateral != null) {
      collateral.insurance_company = value;
      return;
    }
    if ("mortgagor".equals(key) && collateral != null) {
      collateral.mortgagor = value;
      return;
    }
    if ("percentage".equals(key) && collateral != null) {
      collateral.percentage = new BigDecimal(value);
      return;
    }
    if ("summa".equals(key) && collateral != null) {
      collateral.summa = new BigDecimal(value);
      return;
    }
    if ("summaNT".equals(key) && collateral != null) {
      collateral.summa_nt = new BigDecimal(value);
      return;
    }


    //read phone fields
    if ("phoneId".equals(key) && phone != null) {
      phone.phone_id = value;
      return;
    }
    if ("phoneNumStatus".equals(key) && phone != null) {
      phone.phone_num_status = value;
      return;
    }
    if ("phoneNumType".equals(key) && phone != null) {
      phone.phone_num_type = value;
      return;
    }
    if ("phoneNumb".equals(key) && phone != null) {
      phone.phone_numb = value;
      return;
    }

    //read plan_oper fields
    if ("contractId".equals(key) && planOper != null) {
      planOper.contract_id = value;
      return;
    }
    if ("credSumma".equals(key) && planOper != null) {
      planOper.cred_summa = new BigDecimal(value);
      return;
    }
    if ("debtCredBalance".equals(key) && planOper != null) {
      planOper.debt_cred_balance = new BigDecimal(value);
      return;
    }
    if ("dogSumma".equals(key) && planOper != null) {
      planOper.dog_summa = new BigDecimal(value);
      return;
    }
    if ("monthSumma".equals(key) && planOper != null) {
      planOper.month_summa = new BigDecimal(value);
      return;
    }
    if ("prcSumma".equals(key) && planOper != null) {
      planOper.prc_summa = new BigDecimal(value);
      return;
    }
    if ("valuta".equals(key) && planOper != null) {
      planOper.valuta = value;
      return;
    }

  }

}
