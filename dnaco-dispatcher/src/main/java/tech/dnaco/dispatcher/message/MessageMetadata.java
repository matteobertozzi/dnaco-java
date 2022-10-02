package tech.dnaco.dispatcher.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import tech.dnaco.collections.arrays.ArrayUtil;
import tech.dnaco.strings.StringConverter;

public interface MessageMetadata {
  int size();
  boolean isEmpty();

  String get(String key);
  List<String> getList(String key);

  void forEach(final BiConsumer<? super String, ? super String> action);

  // --------------------------------------------------------------------------------
  // Property helpers
  // --------------------------------------------------------------------------------
  default String getString(final String key, final String defaultValue) {
    final String value = get(key);
    return value != null ? value : defaultValue;
  }

  default int getInt(final String key, final int defaultValue) {
    return StringConverter.toInt(key, get(key), defaultValue);
  }

  default long getLong(final String key, final long defaultValue) {
    return StringConverter.toLong(key, get(key), defaultValue);
  }

  default float getFloat(final String key, final float defaultValue) {
    return StringConverter.toFloat(key, get(key), defaultValue);
  }

  default double getDouble(final String key, final double defaultValue) {
    return StringConverter.toDouble(key, get(key), defaultValue);
  }

  default boolean getBoolean(final String key, final boolean defaultValue) {
    return StringConverter.toBoolean(key, get(key), defaultValue);
  }

  default <T extends Enum<T>> T getEnumValue(final Class<T> enumType, final String key, final T defaultValue) {
    return StringConverter.toEnumValue(enumType, key, get(key), defaultValue);
  }

  // --------------------------------------------------------------------------------
  // List lookup helpers
  // --------------------------------------------------------------------------------
  default String[] getStringList(final String key, final String[] defaultValue) {
    return StringConverter.toStringList(key, get(key), defaultValue);
  }

  default Set<String> getStringSet(final String key, final String[] defaultValue) {
    final String[] items = getStringList(key, defaultValue);
    return ArrayUtil.isEmpty(items) ? Set.of() : Set.of(items);
  }

  default int[] getIntList(final String key, final int[] defaultValue) {
    return StringConverter.toIntList(key, get(key), defaultValue);
  }

  default String[] toStringArray() {
    final ArrayList<String> kvs = new ArrayList<>(size());
    forEach((k, v) -> {
      kvs.add(k);
      kvs.add(v);
    });
    return kvs.toArray(new String[0]);
  }
}
