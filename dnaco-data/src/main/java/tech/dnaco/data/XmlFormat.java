package tech.dnaco.data;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import tech.dnaco.data.json.JsonArray;
import tech.dnaco.data.json.JsonElement;
import tech.dnaco.data.json.JsonObject;

public class XmlFormat extends DataFormat {
  public static final XmlFormat INSTANCE = new XmlFormat();

  private static final ThreadLocal<XmlFormatMapper> mapper = ThreadLocal.withInitial(XmlFormatMapper::new);

  private XmlFormat() {
    // no-op
  }

  @Override
  protected DataFormatMapper get() {
    return mapper.get();
  }

  private static final class XmlFormatMapper extends DataFormatMapper {
    private XmlFormatMapper() {
      super(new XmlMapper());
    }
  }

  public static void main(final String[] args) {
    final JsonObject json = new JsonObject();
    json.add("a", 10);
    json.add("b", "foo");
    json.add("c", new JsonArray().add(1).add(2));
    json.add("d", new JsonObject().add("d1", 1).add("d2", 2));
    final String xml = XmlFormat.INSTANCE.asString(json);
    System.out.println(xml);
    System.out.println(XmlFormat.INSTANCE.fromString(xml, JsonElement.class));
  }
}
