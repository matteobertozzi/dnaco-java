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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class Filter {
  public enum FilterType {
    OR, AND,
    EQ, NE,
    GT, GE,
    LT, LE,
    LIKE, NLIKE, REGEX,
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
  public void setValue(final Object value) { this.value = value; }
  public Object[] getValues() { return values; }
  public void setValues(final Object[] values) { this.values = values; }
  public boolean hasValues() { return values != null; }
  public Filter[] getFilters() { return filters; }
  public boolean hasFilters() { return filters != null; }

  @Override
  public int hashCode() {
    int result = Objects.hash(type, field, value);
    result = 31 * result + Arrays.hashCode(values);
    result = 31 * result + Arrays.hashCode(filters);
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof final Filter filter)) return false;

    return type == filter.type
      && Objects.equals(field, filter.field)
      && Objects.equals(value, filter.value)
      && Arrays.equals(values, filter.values)
      && Arrays.equals(filters, filter.filters);
  }

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


  public String toQueryString() {
    return toQueryString(new StringBuilder()).toString();
  }

  private StringBuilder toQueryString(final StringBuilder builder) {
    if (filters != null) {
      final Filter[] sfilter = Arrays.copyOf(filters, filters.length);
      Arrays.sort(sfilter, Filter::compare);
      builder.append("(");
      for (int i = 0; i < sfilter.length; ++i) {
        if (i > 0) builder.append(" ").append(type).append(" ");
        sfilter[i].toQueryString(builder);
      }
      builder.append(")");
    } else if (values != null) {
      builder.append(field).append(" ").append(type).append(" ?");
    } else {
      builder.append(field).append(" ").append(type).append(" ?");
    }
    return builder;
  }

  private static int compare(final Filter a, final Filter b) {
    if (a.filters != null || b.filters != null) {
      if (a.filters == null) return -1;
      if (b.filters == null) return 1;
      return Integer.compare(a.filters.length, b.filters.length);
    }
    return a.field.compareTo(b.field);
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

    public Builder like(final String fieldName, final String value) {
      return add(new Filter(FilterType.LIKE, fieldName, value));
    }

    public Builder notLike(final String fieldName, final String value) {
      return add(new Filter(FilterType.NLIKE, fieldName, value));
    }

    public Builder regexMatch(final String fieldName, final String value) {
      return add(new Filter(FilterType.REGEX, fieldName, value));
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
