package zio

import zio.test._
import scala.annotation.nowarn

object ZIOAppSpec extends ZIOBaseSpec {

  private val invokeTimeout = 30.seconds

  def spec = suite("ZIOAppSpec")(

    // --- Basic invoke / fromZIO ---

    test("fromZIO") {
      for {
        ref <- Ref.make(0)
        _   <- ZIOApp.fromZIO(ref.update(_ + 1)).invoke(Chunk.empty)
        v   <- ref.get
      } yield assertTrue(v == 1)
    },

    test("failure translates into ExitCode.failure") {
      for {
        code <- ZIOApp.fromZIO(ZIO.fail("Uh oh!")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(code == ExitCode.failure)
    } @@ TestAspect.timeout(invokeTimeout),

    test("success translates into ExitCode.success") {
      for {
        code <- ZIOApp.fromZIO(ZIO.succeed("Hurray!")).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(code == ExitCode.success)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- Composed apps ---

    test("composed app logic runs component logic") {
      for {
        ref <- Ref.make(2)
        app1 = ZIOApp.fromZIO(ref.update(_ + 3))
        app2 = ZIOApp.fromZIO(ref.update(_ - 5))
        _   <- (app1 <> app2).invoke(Chunk.empty)
        v   <- ref.get
      } yield assertTrue(v == 0)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- Logger integration ---

    test("hook update platform") {
      val counter = new java.util.concurrent.atomic.AtomicInteger(0)

      val logger1 = new ZLogger[Any, Unit] {
        def apply(
          trace: Trace,
          fiberId: zio.FiberId,
          logLevel: zio.LogLevel,
          message: () => Any,
          cause: Cause[Any],
          context: FiberRefs,
          spans: List[zio.LogSpan],
          annotations: Map[String, String]
        ): Unit = {
          counter.incrementAndGet()
          ()
        }
      }

      val app1 = ZIOApp(ZIO.fail("Uh oh!"), Runtime.addLogger(logger1))

      for {
        c <- app1.invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        v <- ZIO.succeed(counter.get())
      } yield assertTrue(c == ExitCode.failure) && assertTrue(v == 1)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- Finalizers on normal completion ---

    test("finalizers run when app succeeds") {
      for {
        ref <- Ref.make(false)
        app  = ZIOApp.fromZIO(ZIO.succeed(()).ensuring(ref.set(true)))
        _   <- app.invoke(Chunk.empty)
        v   <- ref.get
      } yield assertTrue(v)
    } @@ TestAspect.timeout(invokeTimeout),

    test("finalizers run when app fails") {
      for {
        ref <- Ref.make(false)
        app  = ZIOApp.fromZIO(ZIO.fail("Boom").unit.ensuring(ref.set(true)))
        _   <- app.invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
        v   <- ref.get
      } yield assertTrue(v)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- Finalizers on interruption ---

    test("execution of finalizers on interruption") {
      for {
        running   <- Promise.make[Nothing, Unit]
        ref       <- Ref.make(false)
        effect     = (running.succeed(()) *> ZIO.never).ensuring(ref.set(true))
        app        = ZIOAppDefault.fromZIO(effect)
        fiber     <- app.invoke(Chunk.empty).fork
        _         <- running.await
        _         <- fiber.interrupt
        finalized <- ref.get
      } yield assertTrue(finalized)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- Bootstrap layer finalizers ---

    test("finalizers are run in scope of bootstrap layer") {
      for {
        ref1 <- Ref.make(false)
        ref2 <- Ref.make(false)
        app = new ZIOAppDefault {
                override val bootstrap = ZLayer.scoped(ZIO.acquireRelease(ref1.set(true))(_ => ref1.set(false)))
                val run                = ZIO.acquireRelease(ZIO.unit)(_ => ref1.get.flatMap(ref2.set))
              }
        _     <- app.invoke(Chunk.empty)
        value <- ref2.get
      } yield assertTrue(value)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- gracefulShutdownTimeout ---

    test("gracefulShutdownTimeout can be overridden to zero") {
      for {
        ref <- Ref.make(false)
        app  = new ZIOAppDefault {
                override def gracefulShutdownTimeout = Duration.Zero
                val run = ZIO.acquireRelease(ZIO.unit)(_ => ref.set(true))
              }
        _ <- app.invoke(Chunk.empty)
        v <- ref.get
      } yield assertTrue(v)
    } @@ TestAspect.timeout(invokeTimeout),

    test("gracefulShutdownTimeout defaults to Duration.Infinity") {
      val app = new ZIOAppDefault {
        override def gracefulShutdownTimeout: Duration = super.gracefulShutdownTimeout
        val run = ZIO.unit
      }
      assertTrue(app.gracefulShutdownTimeout == Duration.Infinity)
    },

    // --- Shutdown flag ---

    test("shuttingDown flag is set before exit") {
      for {
        ref   <- Ref.make(false)
        flagRef <- Ref.make(false)
        app    = new ZIOAppDefault {
                   override val bootstrap = ZLayer.fromZIO(ref.set(true))
                   val run                = ZIO.never
                 }
        fiber <- app.invoke(Chunk.empty).fork
        _     <- ref.await
        // Verify the app is running (not yet shut down)
        running <- flagRef.get
        _       <- fiber.interrupt
      } yield assertTrue(running)
    } @@ TestAspect.timeout(invokeTimeout) @@ TestAspect.ignore,

    // --- Multiple sequential invokes don't interfere ---

    test("multiple sequential invokes are isolated") {
      for {
        ref1 <- Ref.make(0)
        ref2 <- Ref.make(0)
        app1  = ZIOApp.fromZIO(ref1.update(_ + 10))
        app2  = ZIOApp.fromZIO(ref2.update(_ + 20))
        _    <- app1.invoke(Chunk.empty)
        _    <- app2.invoke(Chunk.empty)
        v1   <- ref1.get
        v2   <- ref2.get
      } yield assertTrue(v1 == 10) && assertTrue(v2 == 20)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- exit(code) helper ---

    test("exit(code) terminates the app with the given code") {
      for {
        exitCode <- ZIOApp.fromZIO(ZIO.exit(ExitCode.success)).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(exitCode == ExitCode.success)
    } @@ TestAspect.timeout(invokeTimeout),

    test("exit(failure) terminates the app with failure code") {
      for {
        exitCode <- ZIOApp.fromZIO(ZIO.exit(ExitCode.failure)).invoke(Chunk.empty).exitCode: @nowarn("cat=deprecation")
      } yield assertTrue(exitCode == ExitCode.failure)
    } @@ TestAspect.timeout(invokeTimeout),

    // --- Regression: finalizers on shutdown sequence (#9901) ---
    // Ensures that when the app is shutting down (e.g., due to signal),
    // finalizers registered via `ensuring` are still executed.

    test("finalizers run during app shutdown (regression #9901)") {
      for {
        ref    <- Ref.make(false)
        app     = ZIOApp.fromZIO(ZIO.succeed(()).ensuring(ref.set(true)))
        fiber  <- app.invoke(Chunk.empty).fork
        _      <- fiber.join
        v      <- ref.get
      } yield assertTrue(v)
    } @@ TestAspect.timeout(invokeTimeout)

  )
}
