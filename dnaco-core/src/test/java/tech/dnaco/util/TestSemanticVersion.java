package tech.dnaco.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSemanticVersion {
  @Test
  public void testSimple() {
    final long v = SemanticVersion.compose(123, 4567, 987654);
    Assertions.assertEquals(1081957753491974L, v);
    Assertions.assertEquals(987654, SemanticVersion.patch(v));
    Assertions.assertEquals(4567, SemanticVersion.minor(v));
    Assertions.assertEquals(123, SemanticVersion.major(v));
    Assertions.assertEquals("123.4567.987654", SemanticVersion.toString(v));
  }
}
