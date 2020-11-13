package tech.dnaco.util;

import java.util.concurrent.atomic.AtomicBoolean;

import tech.dnaco.logging.Logger;
import tech.dnaco.logging.LoggerSession;
import tech.dnaco.strings.HumansUtil;

public final class ShutdownUtil {
  private ShutdownUtil() {
    // no-op
  }

  public interface StopSignal {
    boolean sendStopSignal();
  }

  public static void addShutdownHook(final String name, StopSignal... services) {
    addShutdownHook(name, Thread.currentThread(), services);
  }

  public static void addShutdownHook(final String name, final Thread mainThread, StopSignal... services) {
    addShutdownHook(name, mainThread, new AtomicBoolean(true), services);
  }

  public static void addShutdownHook(final String name, final AtomicBoolean running, StopSignal... services) {
    addShutdownHook(name, Thread.currentThread(), running, services);
  }

  public static void addShutdownHook(final String name, final Thread mainThread, final AtomicBoolean running, StopSignal... services) {
    Runtime.getRuntime().addShutdownHook(new Thread(name + "ShutdownHook") {
      @Override
      public void run() {
        Logger.setSession(LoggerSession.newSystemGeneralSession());
        final long startTime = System.nanoTime();
        Logger.info("{} shutdown hook!", name);
        for (int i = 0; i < services.length; ++i) {
          services[i].sendStopSignal();
        }
        running.set(false);
        Logger.debug("waiting for {} to finish", mainThread);
        ThreadUtil.shutdown(mainThread);
        Logger.info("shutdown took: {}", HumansUtil.humanTimeSince(startTime));
      }
    });
  }
}