/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tech.dnaco.storage.query;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import tech.dnaco.bytes.ByteArraySlice;
import tech.dnaco.bytes.BytesSlice;
import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.collections.caches.LruCache;
import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonObject;
import tech.dnaco.storage.DataTypes.DataType;
import tech.dnaco.storage.EntityRow;
import tech.dnaco.storage.Schema;
import tech.dnaco.storage.format.FieldFormatReader;
import tech.dnaco.storage.query.Filter.FilterType;
import tech.dnaco.strings.StringUtil;

public final class Query {
  private static final QueryCache QUERY_CACHE = new QueryCache();

  private final OptimizerFilter filter;
  private final EntityRow row;

  public Query(final Schema schema, final Filter filter) {
    this(schema, compile(new HashMap<>(32), schema, filter));
  }

  public Query(final Schema schema, final OptimizerFilter filter) {
    this.filter = filter;
    this.row = new EntityRow(schema);
    extractKeyPrefixes();
  }

  public boolean process(final byte[] key, final FieldFormatReader fieldReader) {
    row.load(key, fieldReader);
    return filter.process(row);
  }

  public record KeyField (int keyIndex, OptimizerFieldChecker checker) {}

  private void extractKeyPrefixes() {
    final HashSet<OptimizerFilter> processed = new HashSet<>();
    final Schema schema = row.schema();
    if (filter instanceof final OptimizerConjunction conjunction) {
      extractKeyPrefixes(processed, schema, conjunction);
    } else {
      throw new UnsupportedOperationException("unexpected filter type: " + filter);
    }
  }

  private void extractKeyPrefixes(final HashSet<OptimizerFilter> processed,
      final Schema schema, final OptimizerConjunction conjunction) {
    if (!processed.add(conjunction)) return;

    final ArrayList<KeyField> keys = new ArrayList<>();
    for (final OptimizerFilter f: conjunction.filters) {
      if (f instanceof final OptimizerFieldChecker fieldChecker) {
        final int keyIndex = schema.isKey(fieldChecker.getFieldIndex());
        if (keyIndex >= 0) keys.add(new KeyField(keyIndex, fieldChecker));
      } else if (f instanceof final OptimizerConjunction subConjunction) {
        extractKeyPrefixes(processed, schema, subConjunction);
      }
    }

    if (keys.isEmpty()) {
      return;
    }

    keys.sort((a, b) -> Integer.compare(a.keyIndex(), b.keyIndex()));
    if (keys.get(0).keyIndex() > 0) {
      // we need the first key element
      return;
    }

    System.out.println(" -> " + keys);
  }

  private static OptimizerFilter compile(final HashMap<Filter, OptimizerFilter> uniqFilters,
      final Schema schema, final Filter filter) {
    final OptimizerFilter other = uniqFilters.get(filter);
    if (other != null) return other;

    final OptimizerFilter optimizedFilter;
    if (filter.hasFilters()) {
      final Filter[] subFilters = filter.getFilters();
      final OptimizerFilter[] optimizerSubFilters = new OptimizerFilter[subFilters.length];
      optimizedFilter = new OptimizerConjunction(filter.getType(), optimizerSubFilters);
      for (int i = 0; i < subFilters.length; ++i) {
        optimizerSubFilters[i] = compile(uniqFilters, schema, subFilters[i]);
      }
      Arrays.sort(optimizerSubFilters, (a, b) -> filterSorter(schema, a, b));
    } else {
      final int fieldIndex = schema.fieldByName(filter.getField());
      optimizedFilter = switch (filter.getType()) {
        case EMPTY, NEMPTY -> new OptimizerEmpty(filter.getType(), fieldIndex);
        case EQ, NE, GE, GT, LE, LT -> new OptimizerComparator(filter.getType(), fieldIndex, filter.getValue());
        case LIKE, NLIKE -> new OptimizerLike(filter.getType(), fieldIndex, (String) filter.getValue());
        case IN, NIN -> new OptimizerIn(filter.getType(), fieldIndex, filter.getValues());
        case BW -> new OptimizerBetween(fieldIndex, filter.getValues());
        case AND, OR -> throw new IllegalArgumentException("Unexpected value: " + filter.getType());
      };
    }
    uniqFilters.put(filter, optimizedFilter);
    return optimizedFilter;
  }

  private static int filterSorter(final Schema schema, final OptimizerFilter a, final OptimizerFilter b) {
    if (a instanceof OptimizerConjunction || b instanceof OptimizerConjunction) {
      if (!(a instanceof OptimizerConjunction)) return -1;
      if (!(b instanceof OptimizerConjunction)) return 1;
      return Integer.compare(((OptimizerConjunction)a).length(), ((OptimizerConjunction)b).length());
    }

    final OptimizerFieldChecker fa = ((OptimizerFieldChecker)a);
    final OptimizerFieldChecker fb = ((OptimizerFieldChecker)b);
    final int aKey = schema.isKey(fa.getFieldIndex());
    final int bKey = schema.isKey(fb.getFieldIndex());
    if (aKey >= 0 || bKey >= 0) {
      if (aKey < 0) return 1;
      if (bKey < 0) return -1;
      return Integer.compare(aKey, bKey);
    }
    // TODO: sort by fast compare and then field index
    return Integer.compare(fa.getFieldIndex(), fb.getFieldIndex());
  }

  private static abstract class OptimizerFilter {
    private Boolean result;

    protected boolean process(final EntityRow row) {
      if (result != null) {
        //System.out.println("CACHED " + this + " -> result: " + result);
        return result;
      }

      this.result = compute(row);
      //System.out.println("COMPUTE " + this + " -> result: " + result);
      return result;
    }

    protected abstract boolean compute(EntityRow row);
  }

  private static abstract class OptimizerFieldChecker extends OptimizerFilter {
    protected abstract int getFieldIndex();
  }

  private static class OptimizerConjunction extends OptimizerFilter {
    private final OptimizerFilter[] filters;
    private final FilterType type;

    private OptimizerConjunction(final FilterType type, final OptimizerFilter[] filters) {
      this.type = type;
      this.filters = filters;
    }

    public int length() {
      return filters.length;
    }

    @Override
    protected boolean compute(final EntityRow row) {
      if (type == FilterType.OR) {
        for (int i = 0; i < filters.length; ++i) {
          if (filters[i].process(row)) {
            return true;
          }
        }
        return false;
      } else {
        for (int i = 0; i < filters.length; ++i) {
          if (!filters[i].process(row)) {
            return false;
          }
        }
        return true;
      }
    }

    @Override
    public String toString() {
      return "Filter-" + type + ", filters=" + Arrays.toString(filters);
    }
  }

  private static class OptimizerEmpty extends OptimizerFieldChecker {
    private final int fieldIndex;
    private final boolean empty;

    private OptimizerEmpty(final FilterType filterType, final int fieldIndex) {
      this.fieldIndex = fieldIndex;
      this.empty = filterType == FilterType.EMPTY;
    }

    @Override
    protected int getFieldIndex() {
      return fieldIndex;
    }

    @Override
    protected boolean compute(final EntityRow row) {
      final Object value = row.get(fieldIndex);
      if (value == null) return empty;

      return switch (row.getFieldType(fieldIndex)) {
        case BYTES -> ((ByteArraySlice)value).isEmpty() == empty;
        case STRING -> ((String)value).isEmpty() == empty;
        case ARRAY -> ((JsonArray)value).isEmpty() == empty;
        case OBJECT -> ((JsonObject)value).isEmpty() == empty;
        default -> false;
      };
    }

    @Override
    public String toString() {
      return "Filter-" + (empty ? "EMPTY" : "NOT-EMPTY") + " [fieldIndex:" + fieldIndex + "]";
    }
  }

  private static class OptimizerComparator extends OptimizerFieldChecker {
    private final FilterType type;
    private final int fieldIndex;
    private final Object value;

    private OptimizerComparator(final FilterType filterType, final int fieldIndex, final Object value) {
      this.type = filterType;
      this.fieldIndex = fieldIndex;
      this.value = value;
    }

    @Override
    protected int getFieldIndex() {
      return fieldIndex;
    }

    @Override
    protected boolean compute(final EntityRow row) {
      final int cmp = compare(row.getFieldType(fieldIndex), row.get(fieldIndex), value);
      return switch (type) {
        case EQ -> cmp == 0;
        case NE -> cmp != 0;
        case GE -> cmp >= 0;
        case GT -> cmp > 0;
        case LE -> cmp <= 0;
        case LT -> cmp < 0;
        default -> throw new IllegalArgumentException("Unexpected value: " + type);
      };
    }

    @Override
    public String toString() {
      return "Filter-" + type + " [field=" + fieldIndex + ", value=" + value + "]";
    }
  }

  private static int compare(final DataType type, final Object a, final Object b) {
    if (a == null || b == null) {
      return (a != null) ? 1 : (b != null) ? - 1 : 0;
    }

    return switch (type) {
      case BOOL -> Boolean.compare((Boolean) a, (Boolean) b);
      case INT, UTC_TIMESTAMP -> Long.compare(((Number) a).longValue(), ((Number) b).longValue());
      case FLOAT -> Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
      case STRING -> StringUtil.compare((String) a, (String) b);
      case BYTES -> {
        final ByteArraySlice aSlice = (ByteArraySlice)a;
        if (b instanceof final byte[] bArray) {
          yield BytesUtil.compare(aSlice.rawBuffer(), aSlice.offset(), aSlice.length(), bArray, 0, BytesUtil.length(bArray));
        } else if (b instanceof final BytesSlice bSlice) {
          yield aSlice.compareTo(bSlice);
        }
        throw new UnsupportedOperationException();
      }
      default -> throw new UnsupportedOperationException();
    };
  }

  private static class OptimizerLike extends OptimizerFieldChecker {
    private final int fieldIndex;
    private final boolean match;
    private final String expr;

    private OptimizerLike(final FilterType filterType, final int fieldIndex, final String expr) {
      this.fieldIndex = fieldIndex;
      this.match = (filterType == FilterType.LIKE);
      this.expr = expr;
    }

    @Override
    protected int getFieldIndex() {
      return fieldIndex;
    }

    @Override
    protected boolean compute(final EntityRow row) {
      final Object value = row.get(fieldIndex);
      if (value == null || expr == null) return !match;

      final String sValue = String.valueOf(value);
      if (sValue.isEmpty() && expr.isEmpty()) {
        return match;
      }

      final Pattern pattern = QUERY_CACHE.likePattern(expr);
      return pattern.matcher(sValue).matches() == match;
    }

    @Override
    public String toString() {
      return "Filter-" + (match ? "LIKE" : "NOT-LIKE") + " [field=" + fieldIndex + ", expr=" + expr + "]";
    }
  }

  private static class OptimizerIn extends OptimizerFieldChecker {
    private final Set<Object> values;
    private final int fieldIndex;
    private final boolean match;

    private OptimizerIn(final FilterType filterType, final int fieldIndex, final Object[] values) {
      this.values = Set.of(values);
      this.fieldIndex = fieldIndex;
      this.match = (filterType == FilterType.IN);
    }

    @Override
    protected int getFieldIndex() {
      return fieldIndex;
    }

    @Override
    protected boolean compute(final EntityRow row) {
      // TODO: Long vs Integer problem...
      final Object value = row.get(fieldIndex);
      return this.values.contains(value) == match;
    }

    @Override
    public String toString() {
      return "Filter-" + (match ? "IN" : "NOT-IN") + " [field=" + fieldIndex + ", values=" + values + "]";
    }
  }

  private static class OptimizerBetween extends OptimizerFieldChecker {
    private final int fieldIndex;
    private final Object[] values;

    private OptimizerBetween(final int fieldIndex, final Object[] values) {
      this.fieldIndex = fieldIndex;
      this.values = values;
    }

    @Override
    protected int getFieldIndex() {
      return fieldIndex;
    }

    @Override
    protected boolean compute(final EntityRow row) {
      final DataType type = row.getFieldType(fieldIndex);
      final Object value = row.get(fieldIndex);
      return compare(type, value, values[0]) >= 0 && compare(type, value, values[1]) <= 0;
    }

    @Override
    public String toString() {
      return "Filter-BETWEEN [field=" + fieldIndex + ", values=" + Arrays.toString(values) + "]";
    }
  }

  public static class QueryCache {
    private final LruCache<String, Pattern> likeCache = new LruCache<>(128, 512, Duration.ofMinutes(5));

    public Pattern likePattern(final String expr) {
      return likeCache.computeIfAbsent(expr, StringUtil::likePattern);
    }
  }
}
