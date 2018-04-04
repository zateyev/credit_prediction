package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

public class ContractsRespParserTest {
  @Test
  public void testName() throws Exception {
    try (
      Connection connection = DbAccess.createConnection();
      ContractsRespParser p = new ContractsRespParser(connection, 10_000)
      ) {

      Path path = Paths.get("/home/zateyev/Gshare/credit_prediction/raw_data/getContracts/05-00-28-2873417081.txt");
      FileInputStream fileInputStream = new FileInputStream(path.toFile());
      p.read(fileInputStream);

    }
  }
}