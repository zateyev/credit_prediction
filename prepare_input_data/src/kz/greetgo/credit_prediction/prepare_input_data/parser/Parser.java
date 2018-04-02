package kz.greetgo.credit_prediction.prepare_input_data.parser;

public interface Parser {
  void startBracket(String localName);
  void endBracket(String localName);
}
