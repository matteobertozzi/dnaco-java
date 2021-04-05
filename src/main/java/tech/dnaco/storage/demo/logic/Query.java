package tech.dnaco.storage.demo.logic;

import tech.dnaco.bytes.BytesUtil;
import tech.dnaco.storage.demo.EntityDataRow;
import tech.dnaco.storage.demo.EntityDataType;
import tech.dnaco.storage.demo.Filter;
import tech.dnaco.strings.StringUtil;

public final class Query {
  private Query() {
    // no-op
  }

  private static int compare(final EntityDataType type, final Object a, final Object b) {
    if (a == null || b == null) {
      if (a != null && b == null) return 1;
      if (a == null && b != null) return -1;
      return 0;
    }

    switch (type) {
      case BOOL: return Boolean.compare((Boolean)a, (Boolean)b);
      case INT: return Long.compare(((Number)a).longValue(), ((Number)b).longValue());
      case FLOAT: return Double.compare(((Number)a).doubleValue(), ((Number)b).doubleValue());
      case STRING: return StringUtil.compareTo((String)a, (String)b);
      case BYTES: return BytesUtil.compare((byte[])a, (byte[])b);
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static boolean like(final Object value, final Object exp) {
    return StringUtil.like((String) value, (String) exp);
  }

  private static boolean isEmpty(final Object value) {
    if (value == null) return true;

    if (value instanceof String) {
      return StringUtil.isEmpty((String)value);
    } else if (value instanceof byte[]) {
      return BytesUtil.isEmpty((byte[])value);
    }
    return false;
  }

  private static boolean in(final EntityDataType type, final Object value, final Object[] values) {
    for (int i = 0; i < values.length; ++i) {
      if (compare(type, value, values[i]) == 0) {
        return true;
      }
    }
    return false;
  }

  private static boolean between(final EntityDataType type, final Object value, final Object[] values) {
    return compare(type, value, values[0]) >= 0 && compare(type, value, values[1]) <= 0;
  }

  public static boolean process(final Filter filter, final EntityDataRow row) {
    switch (filter.getType()) {
      case OR:
        final Filter[] orFilter = filter.getFilters();
        for (int i = 0; i < orFilter.length; ++i) {
          if (process(orFilter[i], row)) {
            return true;
          }
        }
        return false;
      case AND: {
        final Filter[] andFilter = filter.getFilters();
        for (int i = 0; i < andFilter.length; ++i) {
          if (!process(andFilter[i], row)) {
            return false;
          }
        }
        return true;
      }

      case EQ: return compare(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValue()) == 0;
      case NE: return compare(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValue()) != 0;
      case GT: return compare(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValue()) > 0;
      case GE: return compare(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValue()) >= 0;
      case LT: return compare(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValue()) < 0;
      case LE: return compare(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValue()) <= 0;
      case LIKE: return like(row.getObject(filter.getField()), filter.getValue());
      case NLIKE: return !like(row.getObject(filter.getField()), filter.getValue());
      case EMPTY: return isEmpty(row.getObject(filter.getField()));
      case NEMPTY: return !isEmpty(row.getObject(filter.getField()));
      case IN: return in(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValues());
      case NIN: return !in(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValues());
      case BW: return between(row.getType(filter.getField()), row.getObject(filter.getField()), filter.getValues());
    }
    return false;
  }
}
