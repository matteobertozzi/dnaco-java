package tech.dnaco.dispatcher;

@FunctionalInterface
public interface Invokable {
  Object invoke() throws Throwable;
}
