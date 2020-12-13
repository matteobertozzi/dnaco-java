package tech.dnaco.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SetUtil {
  private SetUtil() {
    // no-op
  }

  public static <T> int size(final Set<T> input) {
    return input != null ? input.size() : 0;
  }

  public static <T> Set<T> emptyIfNull(final Set<T> input) {
    return input == null ? Collections.emptySet() : input;
  }

  public static <T> boolean isEmpty(final Collection<T> input) {
    return input == null || input.isEmpty();
  }

  public static <T> boolean isNotEmpty(final Collection<T> input) {
    return input != null && !input.isEmpty();
  }

  public static boolean contains(final Set<String> set, final String value) {
    return set != null && set.contains(value);
  }

  public static boolean containsOneOf(final Set<String> set, final String... value) {
    if (isEmpty(set)) return false;

    for (int i = 0; i < value.length; ++i) {
      if (set.contains(value[i])) {
        return true;
      }
    }
    return false;
  }

  public static Set<String> newHashSet(final String... items) {
    return new HashSet<>(List.of(items));
  }

  public static void andAndEnsureLimit(final Set<String> set, final int limit, final String valueToAdd) {
    if (!set.add(valueToAdd)) return; // item was already in

    if (set.size() > limit) {
      set.remove(valueToAdd);
      throw new UnsupportedOperationException("unable to insert " + valueToAdd +
        ", the set has already reached the limit of " + limit);
    }
  }
}
