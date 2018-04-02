package kz.greetgo.credit_prediction.prepare_input_data.parser;

import java.io.InputStream;

public class LogReader {

  private Parser contentHandler;

  public void setContentHandler(ParserImpl contentHandler) {
    this.contentHandler = contentHandler;
  }

  public void parse(InputStream inputStream) {

  }
}
