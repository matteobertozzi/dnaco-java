# Time
For Unit-Tests it is useful to increment time to simulate timeouts. By using TimeUtil.currentUtcMillis() in your code you can just specify a *ClockProvider* like *ManualClockProvider* to manually manipulate the time, without having to change your code or use sleeps.

```java
{
  ManualClockProvider clock = new ManualClockProvider();
  TimeUtil.setClockProvider(clock);

  ...
  myFunction();
  clock.incTime(5, TimeUnit.MINUTES);
  myFunction();
  ...
}
```