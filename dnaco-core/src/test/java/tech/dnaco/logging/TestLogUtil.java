package tech.dnaco.logging;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.dnaco.logging.LogUtil.LogLevel;

public class TestLogUtil {
  @Test
  public void testLogLevelOrdinal() {
    for (final LogLevel level: LogLevel.values()) {
      Assertions.assertEquals(level, LogUtil.levelFromOrdinal(level.ordinal()));
    }
  }
}
