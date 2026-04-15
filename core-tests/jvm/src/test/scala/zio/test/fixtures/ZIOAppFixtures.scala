package zio.test.fixtures

import zio._

/** Exits immediately with success */
object SuccessfulZIOApp extends ZIOAppDefault {
  def run: ZIO[Any, Any, Any] = ZIO.succeed(println("APP_SUCCESS"))
}

/** Exits immediately with failure */
object FailingZIOApp extends ZIOAppDefault {
  def run: ZIO[Any, Any, Any] = ZIO.fail(println("APP_FAILED"))
}

/** Successful completion with finalizer */
object FinalizerOnSuccessApp extends ZIOAppDefault {
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIO.acquireReleaseExit(ZIO.succeed(()))((_, _) =>
      ZIO.succeed(println("FINALIZER_RAN"))
    ) *>
      ZIO.succeed(println("APP_SUCCESS"))
}

/** Failed completion with finalizer */
object FinalizerOnFailureApp extends ZIOAppDefault {
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIO.acquireReleaseExit(ZIO.succeed(()))((_, _) =>
      ZIO.succeed(println("FINALIZER_RAN"))
    ) *>
      ZIO.fail(new RuntimeException("APP_FAILED"))
}

/** App that runs forever (ZIO.never) — for signal/hang tests */
object BlockingApp extends ZIOAppDefault {
  def run: ZIO[Any, Any, Any] = ZIO.never
}

/** App with a slow finalizer (5000ms) and default gracefulShutdownTimeout (~3s) */
object SlowFinalizerApp extends ZIOAppDefault {
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIO.acquireReleaseExit(ZIO.succeed(()))((_, _) =>
      ZIO.succeed(println("SLOW_FINALIZER_STARTED")).delay(5.seconds).as(())
    ) *>
      ZIO.succeed(println("STARTING")) *>
      ZIO.never
}

/** App with slow finalizer and gracefulShutdownTimeout = Infinity */
object SlowFinalizerInfinityApp extends ZIOAppDefault {
  override val gracefulShutdownTimeout: Duration = Duration.Infinity
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIO.acquireReleaseExit(ZIO.succeed(()))((_, _) =>
      ZIO.succeed(println("SLOW_FINALIZER_STARTED"))
        .delay(5.seconds) *>
        ZIO.succeed(println("SLOW_FINALIZER_COMPLETED"))
    ) *>
      ZIO.succeed(println("STARTING")) *>
      ZIO.never
}

/** App with finalizer that runs on SIGTERM */
object FinalizerOnSigtermApp extends ZIOAppDefault {
  private val counter = new java.util.concurrent.atomic.AtomicInteger(0)
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIO.acquireReleaseExit(
      ZIO.succeed(counter.incrementAndGet())
    )((count, _) =>
      ZIO.succeed(println(s"FINALIZER_RAN: count=$count"))
    ) *>
      ZIO.never
}

/** Regression #9901: finalizer should complete with Infinity timeout */
object Regression9901App extends ZIOAppDefault {
  override val gracefulShutdownTimeout: Duration = Duration.Infinity

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIO.acquireReleaseExit(
      ZIO.succeed(println("Service is running"))
    )((_, _) =>
      ZIO.succeed(println("Service is closing...")) *>
        ZIO.sleep(3.seconds) *>
        ZIO.succeed(println("Service is closed"))
    ) *>
      ZIO.never
}

/** Regression #9807: clean shutdown without stderr noise */
object Regression9807App extends ZIOAppDefault {
  java.lang.Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      Thread.sleep(2000L)
    }
  })

  def run: ZIO[Any, Any, Any] = ZIO.never
}
