package tech.dnaco.storage.demo;

import java.util.ArrayList;
import java.util.Arrays;

public class Filter {
  public enum FilterType {
    OR, AND,
    EQ, NE,
    GT, GE,
    LT, LE,
    LIKE, NLIKE,
    EMPTY, NEMPTY,
    IN, NIN,
    BW
  }

  private FilterType type;
  private String field;
  private Object value;
  private Object[] values;
  private Filter[] filters;

  public Filter() {
    // no-op
  }

  public Filter(final FilterType type, final Filter[] filters) {
    this.type = type;
    this.filters = filters;
  }

  public Filter(final FilterType type, final String field) {
    this.type = type;
    this.field = field;
  }

  public Filter(final FilterType type, final String field, final Object value) {
    this.type = type;
    this.field = field;
    this.value = value;
  }

  public Filter(final FilterType type, final String field, final Object[] values) {
    this.type = type;
    this.field = field;
    this.values = values;
  }

  public FilterType getType() { return type; }
  public String getField() { return field; }
  public Object getValue() { return value; }
  public Object[] getValues() { return values; }
  public Filter[] getFilters() { return filters; }

  @Override
  public String toString() {
    if (filters != null) {
      return "Filter-" + type + ", filters=" + Arrays.toString(filters) + "]";
    } else if (values != null) {
      return "Filter-" + type + ", field=" + field + ", values=" + Arrays.toString(values) + "]";
    } else {
      return "Filter-" + type + ", field=" + field + ", value=" + value + "]";
    }
  }

  public static Builder newAndFilterBuilder() {
    return new Builder(FilterType.AND);
  }

  public static Builder newOrFilterBuilder() {
    return new Builder(FilterType.OR);
  }

  public static class Builder {
    private final ArrayList<Filter> filters = new ArrayList<>();
    private final FilterType type;

    private Builder(final FilterType type) {
      this.type = type;
    }

    public Builder add(final Filter filter) {
      filters.add(filter);
      return this;
    }

    public Builder eq(final String fieldName, final Object value) {
      return add(new Filter(FilterType.EQ, fieldName, value));
    }

    public Builder ne(final String fieldName, final Object value) {
      return add(new Filter(FilterType.NE, fieldName, value));
    }

    public Builder gt(final String fieldName, final Object value) {
      return add(new Filter(FilterType.GT, fieldName, value));
    }

    public Builder ge(final String fieldName, final Object value) {
      return add(new Filter(FilterType.GE, fieldName, value));
    }

    public Builder lt(final String fieldName, final Object value) {
      return add(new Filter(FilterType.LT, fieldName, value));
    }

    public Builder le(final String fieldName, final Object value) {
      return add(new Filter(FilterType.LE, fieldName, value));
    }

    public Builder like(final String fieldName, final Object value) {
      return add(new Filter(FilterType.LIKE, fieldName, value));
    }

    public Builder notLike(final String fieldName, final Object value) {
      return add(new Filter(FilterType.NLIKE, fieldName, value));
    }

    public Builder empty(final String fieldName) {
      return add(new Filter(FilterType.EMPTY, fieldName));
    }

    public Builder notEmpty(final String fieldName) {
      return add(new Filter(FilterType.NEMPTY, fieldName));
    }

    public Builder in(final String fieldName, final Object... values) {
      return add(new Filter(FilterType.IN, fieldName, values));
    }

    public Builder notIn(final String fieldName, final Object... values) {
      return add(new Filter(FilterType.NIN, fieldName, values));
    }

    public Builder between(final String fieldName, final Object a, final Object b) {
      return add(new Filter(FilterType.BW, fieldName, new Object[] { a, b }));
    }

    public Filter build() {
      return new Filter(type, filters.toArray(new Filter[0]));
    }
  }
}
