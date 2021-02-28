package tech.dnaco.strings;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringFormat {
  @Test
  public void testFormat() {
    Assertions.assertEquals("", StringFormat.format(""));
    Assertions.assertEquals("abc", StringFormat.format("abc"));
    Assertions.assertEquals("abc", StringFormat.format("abc", 1));
    Assertions.assertEquals("abc 1", StringFormat.format("abc {}", 1));
    Assertions.assertEquals("abc 1 def foo", StringFormat.format("abc {} def {}", 1, "foo"));
    Assertions.assertEquals("abc foo def [1, 2, 3] ghi", StringFormat.format("abc {} def {} ghi", "foo", new int[] { 1, 2, 3 }));
    Assertions.assertEquals("abc {aaa 1 bbb 2}", StringFormat.format("abc {aaa {} bbb {}}", 1, 2));
    Assertions.assertEquals("foo Lazy 1 bar key:10 car", StringFormat.format("foo {} bar {key} car", () -> "Lazy 1", () -> 10));
  }

  @Test
  public void testKeyValFormat() {
    Assertions.assertEquals("abc xyz:1", StringFormat.format("abc {xyz}", 1));
    Assertions.assertEquals("abc xyz:1 def foo", StringFormat.format("abc {xyz} def {}", 1, "foo"));
    Assertions.assertEquals("abc xyz:1 def foo ghi wzk:false", StringFormat.format("abc {xyz} def {} ghi {wzk}", 1, "foo", false));
    Assertions.assertEquals("abc {aaa xyz:1 bbb wzk:true}", StringFormat.format("abc {aaa {xyz} bbb {wzk}}", 1, true));
  }

  @Test
  public void testPositionalFormat() {
    Assertions.assertEquals("", StringFormat.positionalFormat(""));
    Assertions.assertEquals("abc", StringFormat.positionalFormat("abc"));
    Assertions.assertEquals("abc", StringFormat.positionalFormat("abc", 1));
    Assertions.assertEquals("abc 1", StringFormat.positionalFormat("abc {0}", 1));
    Assertions.assertEquals("abc 1 foo", StringFormat.positionalFormat("abc {0} {1}", 1, "foo"));
    Assertions.assertEquals("abc foo def 1", StringFormat.positionalFormat("abc {1} def {0}", 1, "foo"));
    Assertions.assertEquals("abc foo def 1 ghi", StringFormat.positionalFormat("abc {1} def {0} ghi", 1, "foo"));
  }
}
