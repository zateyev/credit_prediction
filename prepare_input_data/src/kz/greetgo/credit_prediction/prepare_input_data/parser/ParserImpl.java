package kz.greetgo.credit_prediction.prepare_input_data.parser;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ParserImpl implements Parser {
  private final List<String> pathList = new ArrayList<>();

  public void parse(InputStream inputStream) {
    if (inputStream == null) return;

    LogReader logReader = new LogReader();
    logReader.setContentHandler(this);
    logReader.parse(inputStream);
  }

  public String path() {
    StringBuilder sb = new StringBuilder();
    for (String pathElement : pathList) {
      sb.append('/').append(pathElement);
    }
    return sb.toString();
  }

  @Override
  public void startBracket(String localName) {

  }

  @Override
  public void endBracket(String localName) {

  }
}
