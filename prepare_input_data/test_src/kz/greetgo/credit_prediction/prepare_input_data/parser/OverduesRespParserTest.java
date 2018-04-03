package kz.greetgo.credit_prediction.prepare_input_data.parser;

import kz.greetgo.credit_prediction.prepare_input_data.db.DbAccess;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

public class OverduesRespParserTest {
  @Test
  public void testRead() throws Exception {
    try (
      Connection connection = DbAccess.createConnection();
      OverduesRespParser p = new OverduesRespParser(connection, 10_000)
      ) {

      Path path = Paths.get("/home/zateyev/Gshare/credit_prediction/raw_data/getOverdues/05-34-04-2119601050.txt");
      p.read(path);

    }
  }
}