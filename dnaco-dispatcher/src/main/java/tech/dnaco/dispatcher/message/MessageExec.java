package tech.dnaco.dispatcher.message;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import tech.dnaco.data.DataFormat;
import tech.dnaco.dispatcher.message.MessageHandler.MessageData;
import tech.dnaco.dispatcher.message.UriRouters.UriRoute;
import tech.dnaco.dispatcher.message.UriRouters.UriRoutesBuilder;
import tech.dnaco.strings.HumansUtil;

public class MessageExec {
  public static class FooHandler implements MessageHandler {
    @UriMapping(uri = "/test-get-json")
    public String[] testGetJson() {
      return new String[] { "aaa", "bbb", "ccc" };
    }

    @UriMapping(uri = "/test-static")
    public void testStatic(@HeaderValue("x") final String[] x, final String body) {
      System.out.println("exec testStatic");
      System.out.println(" - queryParam: x=" + Arrays.toString(x));
      System.out.println(" - body: " + body);
    }

    @UriVariableMapping(uri = "/test-variable/{foo}")
    public void testVariable(@UriVariable("foo") final String foo) {
      System.out.println("exec testVariable " + foo);
    }

    @UriPatternMapping(uri = "/test-pattern/(.*)")
    public void testPattern(@UriPattern(0) final String v) {
      System.out.println("exec testPattern " + v);
    }
  }

  private static class SimpleMessageData implements MessageData {
    private final String method;
    private final String path;

    private SimpleMessageData(final String method, final String path) {
      this.method = method;
      this.path = path;
    }

    @Override
    public String method() {
      return method;
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public List<String> queryParamAsList(final String key) {
      if (key.equals("x")) {
        return List.of("yoghi");
      }
      return Collections.emptyList();
    }

    @Override
    public List<String> metadataValueAsList(final String key) {
      if (key.equals("x")) {
        return List.of("Hyoghi");
      }
      return Collections.emptyList();
    }

    @Override
    public String getMetadata(final String key, final String defaultValue) {
      if (key.equals("x")) {
        return "H2yoghi";
      }
      return defaultValue;
    }

    @Override
    public <T> T convertBody(final DataFormat format, final Class<T> classOfT) {
      System.out.println("CONVERT BODY " + classOfT + " " + format);
      return (T)"yello body";
    }
  }

  public static void main(final String[] args) throws Throwable {
    final UriRoutesBuilder routes = new UriRoutesBuilder();
    routes.addHandler(new FooHandler());

    final UriDispatcher dispatcher = new UriDispatcher(routes);
    if (true) {
      final int N = 10_000_000;
      final long startTime = System.nanoTime();
      for (int i = 0; i < N; ++i) {
        dispatcher.exec(new SimpleMessageData("GET", "/test-get-json"));
      }
      final long elapsed = System.nanoTime() - startTime;

      System.out.println(HumansUtil.humanTimeNanos(elapsed) + " -> " + HumansUtil.humanRate((double)N / (elapsed * 0.000000001)));
      return;
    }


    dispatcher.exec(new SimpleMessageData("GET", "/test-static"));
    dispatcher.exec(new SimpleMessageData("GET", "/test-variable/yooo"));
    dispatcher.exec(new SimpleMessageData("GET", "/test-pattern/foo/bar"));

    for (final UriRoute route: routes.getStaticUri()) {
      System.out.println(" -> A: " + route.getUriMethods() + " -> " + route.getUri());
    }
    for (final UriRoute route: routes.getVariableUri()) {
      System.out.println(" -> B: " + route.getUriMethods() + " -> " + route.getUri());
    }
    for (final UriRoute route: routes.getPatternUri()) {
      System.out.println(" -> B: " + route.getUriMethods() + " -> " + route.getUri());
    }
  }
}
