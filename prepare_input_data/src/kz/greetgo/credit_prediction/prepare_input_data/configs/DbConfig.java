package kz.greetgo.credit_prediction.prepare_input_data.configs;

import com.sun.org.glassfish.gmbal.Description;
import kz.greetgo.conf.hot.DefaultStrValue;

@Description("Параметры доступа к БД (используется только БД Postgresql)")
public interface DbConfig {
  @Description("URL доступа к БД")
  @DefaultStrValue("jdbc:postgresql://localhost:5432/asd")
  String url();

  @Description("Пользователь для доступа к БД")
  @DefaultStrValue("asd")
  String username();

  @Description("Пароль для доступа к БД")
  @DefaultStrValue("asd")
  String password();
}
