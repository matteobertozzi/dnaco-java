package tech.dnaco.collections.maps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class TestMapUtil {
  @Test
  public void testMultiMapList() {
    final Map<String, List<String>> mapList = new HashMap<>();
    MapUtil.addToList(mapList, "k1", "v1");
    MapUtil.addToList(mapList, "k2", "v1");
    MapUtil.addToList(mapList, "k1", "v2");
    System.out.println(mapList);
    assertEquals(2, mapList.size());
    assertEquals(2, mapList.get("k1").size());
    assertEquals(1, mapList.get("k2").size());
    assertEquals("v1", mapList.get("k1").get(0));
    assertEquals("v2", mapList.get("k1").get(1));
    assertEquals("v1", mapList.get("k2").get(0));
  }

  @Test
  public void testMultiMapSet() {
    final Map<String, Set<String>> mapList = new HashMap<>();
    MapUtil.addToSet(mapList, "k1", "v1");
    MapUtil.addToSet(mapList, "k2", "v1");
    MapUtil.addToSet(mapList, "k1", "v2");
    System.out.println(mapList);
    assertEquals(2, mapList.size());
    assertEquals(2, mapList.get("k1").size());
    assertEquals(1, mapList.get("k2").size());
    assertEquals(true, mapList.get("k1").contains("v1"));
    assertEquals(true, mapList.get("k1").contains("v2"));
    assertEquals(true, mapList.get("k2").contains("v1"));

    MapUtil.removeFromSet(mapList, "k1", "v1");
    assertEquals(1, mapList.get("k1").size());
    assertEquals(1, mapList.get("k2").size());
    assertEquals(true, mapList.get("k1").contains("v2"));

    MapUtil.removeFromSet(mapList, "k1", "v1");
    MapUtil.removeFromSet(mapList, "k1", "v2");
    assertEquals(0, mapList.get("k1").size());
    assertEquals(1, mapList.get("k2").size());
  }
}
