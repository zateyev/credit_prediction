package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

public class TransactionsRespParserTest {

  @Test
  public void testName() throws Exception {
    try (
      Connection connection = DbAccess.createConnection();
      TransactionsRespParser p = new TransactionsRespParser(connection, 10_000)
    ) {

      Path path = Paths.get("/home/zateyev/Gshare/credit_prediction/raw_data/getTransactions/05-40-45-919867079.txt");
      FileInputStream fileInputStream = new FileInputStream(path.toFile());
      p.read(fileInputStream);

    }
  }
}
