package tech.dnaco.strings;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringUtil {
  // ================================================================================
  //  String length related
  // ================================================================================
  @Test
  public void testLength() {
    Assertions.assertEquals(0, StringUtil.length(null));
    Assertions.assertEquals(0, StringUtil.length(""));
    Assertions.assertEquals(3, StringUtil.length("abc"));
  }

  @Test
  public void testEmpty() {
    Assertions.assertTrue(StringUtil.isEmpty(null));
    Assertions.assertTrue(StringUtil.isEmpty(""));

    Assertions.assertFalse(StringUtil.isNotEmpty(null));
    Assertions.assertFalse(StringUtil.isNotEmpty(""));

    Assertions.assertFalse(StringUtil.isEmpty("abc"));
    Assertions.assertTrue(StringUtil.isNotEmpty("abc"));
  }

  // ================================================================================
  //  String value related
  // ================================================================================
  @Test
  public void testNullAndEmptyConversion() {
    Assertions.assertEquals("", StringUtil.emptyIfNull(null));
    Assertions.assertEquals("", StringUtil.emptyIfNull(""));
    Assertions.assertEquals("abc", StringUtil.emptyIfNull("abc"));

    Assertions.assertNull(StringUtil.nullIfEmpty(null));
    Assertions.assertNull(StringUtil.nullIfEmpty(""));
    Assertions.assertEquals("abc", StringUtil.nullIfEmpty("abc"));
  }

  // ================================================================================
  //  String upper/lower case related
  // ================================================================================
  @Test
  public void testToUpper() {
    Assertions.assertEquals(null, StringUtil.toUpper(null));
    Assertions.assertEquals("", StringUtil.toUpper(""));
    Assertions.assertEquals("UPPER", StringUtil.toUpper("upper"));
    Assertions.assertEquals("UPPER", StringUtil.toUpper("uPpEr"));
    Assertions.assertEquals("UPPER", StringUtil.toUpper("UpPeR"));
    Assertions.assertEquals("UPPER", StringUtil.toUpper("UPPER"));
  }

  @Test
  public void testToLower() {
    Assertions.assertEquals(null, StringUtil.toLower(null));
    Assertions.assertEquals("", StringUtil.toLower(""));
    Assertions.assertEquals("lower", StringUtil.toLower("lower"));
    Assertions.assertEquals("lower", StringUtil.toLower("lOweR"));
    Assertions.assertEquals("lower", StringUtil.toLower("LoWeR"));
    Assertions.assertEquals("lower", StringUtil.toLower("LOWER"));
  }

  // ================================================================================
  //  String trim related
  // ================================================================================
  @Test
  public void testTrim() {
    Assertions.assertEquals(null, StringUtil.trim(null));
    Assertions.assertEquals("", StringUtil.trim(""));
    Assertions.assertEquals("", StringUtil.trim("   "));
    Assertions.assertEquals("", StringUtil.trim(" \t "));
    Assertions.assertEquals("abc \t  def", StringUtil.trim(" \t abc \t  def \t \n "));

    Assertions.assertEquals("", StringUtil.trimToEmpty(null));
    Assertions.assertEquals("", StringUtil.trimToEmpty(""));
    Assertions.assertEquals("", StringUtil.trimToEmpty("   "));
    Assertions.assertEquals("", StringUtil.trimToEmpty(" \t "));
    Assertions.assertEquals("abc \t  def", StringUtil.trimToEmpty(" \t abc \t  def \t \n "));

    Assertions.assertEquals(null, StringUtil.trimToNull(null));
    Assertions.assertEquals(null, StringUtil.trimToNull(""));
    Assertions.assertEquals(null, StringUtil.trimToNull("   "));
    Assertions.assertEquals(null, StringUtil.trimToNull(" \t "));
    Assertions.assertEquals("abc \t  def", StringUtil.trimToNull(" \t abc \t  def \t \n "));

    Assertions.assertEquals(null, StringUtil.ltrim(null));
    Assertions.assertEquals("", StringUtil.ltrim(""));
    Assertions.assertEquals("", StringUtil.ltrim("   "));
    Assertions.assertEquals("abc  ", StringUtil.ltrim("  \t  abc  "));
    Assertions.assertEquals("abc \t  ", StringUtil.ltrim("abc \t  "));

    Assertions.assertEquals(null, StringUtil.rtrim(null));
    Assertions.assertEquals("", StringUtil.rtrim(""));
    Assertions.assertEquals("", StringUtil.rtrim("   "));
    Assertions.assertEquals("  \t  abc", StringUtil.rtrim("  \t  abc  "));
    Assertions.assertEquals("abc", StringUtil.rtrim("abc \t  "));
  }

  @Test
  public void testCollpaseSpaces() {
    Assertions.assertEquals(null, StringUtil.collapseSpaces(null));
    Assertions.assertEquals("", StringUtil.collapseSpaces(""));
    Assertions.assertEquals(" aaa bbb ccc ", StringUtil.collapseSpaces("  aaa    bbb   ccc "));
    Assertions.assertEquals(" aaa bbb ccc ", StringUtil.collapseSpaces("  aaa  \t \t\t  bbb  \t\n\t ccc \t\n"));
  }

  @Test
  public void testSplitAndTrim() {
    Assertions.assertEquals(null, StringUtil.splitAndTrim(null, "/"));
    Assertions.assertEquals(null, StringUtil.splitAndTrim("", "/"));
    Assertions.assertArrayEquals(new String[] { "abc" }, StringUtil.splitAndTrim("abc", "/"));
    Assertions.assertArrayEquals(new String[] { "abc", "def" }, StringUtil.splitAndTrim("abc/def", "/"));
    Assertions.assertArrayEquals(new String[] { "", "abc", "def" }, StringUtil.splitAndTrim("/abc/def/", "/"));
    Assertions.assertArrayEquals(new String[] { "", "abc", "", "def" }, StringUtil.splitAndTrim("/abc//def/", "/"));

    Assertions.assertEquals(null, StringUtil.splitAndTrimSkipEmptyLines(null, "/"));
    Assertions.assertEquals(null, StringUtil.splitAndTrimSkipEmptyLines("", "/"));
    Assertions.assertArrayEquals(new String[] { "abc" }, StringUtil.splitAndTrimSkipEmptyLines("abc", "/"));
    Assertions.assertArrayEquals(new String[] { "abc", "def" }, StringUtil.splitAndTrimSkipEmptyLines("abc/def", "/"));
    Assertions.assertArrayEquals(new String[] { "abc", "def" }, StringUtil.splitAndTrimSkipEmptyLines("/abc/def/", "/"));
    Assertions.assertArrayEquals(new String[] { "abc", "def" }, StringUtil.splitAndTrimSkipEmptyLines("/abc//def/", "/"));
  }

  @Test
  public void testLike() {
    Assertions.assertTrue(StringUtil.like("", "%"));
    Assertions.assertFalse(StringUtil.like(null, "%"));
    Assertions.assertFalse(StringUtil.like("", "abc\\_d"));

    Assertions.assertTrue(StringUtil.like("abc_d", "abc\\_d"));
    Assertions.assertTrue(StringUtil.like("abc%d", "abc\\%%d"));
    Assertions.assertFalse(StringUtil.like("abcd", "abc\\_d"));

    final String source = "1abcd";
    Assertions.assertTrue(StringUtil.like(source, "_%d"));
    Assertions.assertFalse(StringUtil.like(source, "%%a"));
    Assertions.assertFalse(StringUtil.like(source, "1"));
    Assertions.assertTrue(StringUtil.like(source, "%d"));
    Assertions.assertTrue(StringUtil.like(source, "%%%%"));
    Assertions.assertTrue(StringUtil.like(source, "1%_"));
    Assertions.assertFalse(StringUtil.like(source, "1%_2"));
    Assertions.assertFalse(StringUtil.like(source, "1abcdef"));
    Assertions.assertTrue(StringUtil.like(source, "1abcd"));
    Assertions.assertFalse(StringUtil.like(source, "1abcde"));

    Assertions.assertTrue(StringUtil.like(source, "_%_"));
    Assertions.assertTrue(StringUtil.like(source, "_%____"));
    Assertions.assertTrue(StringUtil.like(source, "_____"));
    Assertions.assertFalse(StringUtil.like(source, "___"));
    Assertions.assertFalse(StringUtil.like(source, "__%____"));
    Assertions.assertFalse(StringUtil.like(source, "1"));

    Assertions.assertFalse(StringUtil.like(source, "a_%b"));
    Assertions.assertTrue(StringUtil.like(source, "1%"));
    Assertions.assertFalse(StringUtil.like(source, "d%"));
    Assertions.assertTrue(StringUtil.like(source, "_%"));
    Assertions.assertTrue(StringUtil.like(source, "_abc%"));
    Assertions.assertTrue(StringUtil.like(source, "%d"));
    Assertions.assertTrue(StringUtil.like(source, "%abc%"));
    Assertions.assertFalse(StringUtil.like(source, "ab_%"));

    Assertions.assertTrue(StringUtil.like(source, "1ab__"));
    Assertions.assertTrue(StringUtil.like(source, "1ab__%"));
    Assertions.assertFalse(StringUtil.like(source, "1ab___"));
    Assertions.assertTrue(StringUtil.like(source, "%"));

    Assertions.assertFalse(StringUtil.like(null, "1ab___"));
    Assertions.assertFalse(StringUtil.like(source, null));
    Assertions.assertFalse(StringUtil.like(source, ""));
  }

  // ================================================================================
  //  String comparison related
  // ================================================================================
  @Test
  public void testEquals() {
    Assertions.assertTrue(StringUtil.equals(null, null));
    Assertions.assertFalse(StringUtil.equals(null, ""));
    Assertions.assertFalse(StringUtil.equals("", null));
    Assertions.assertTrue(StringUtil.equals("", ""));
    Assertions.assertFalse(StringUtil.equals(null, "abc"));
    Assertions.assertFalse(StringUtil.equals("abc", null));
  }
}
