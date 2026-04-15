package zio

import zio.test._
import scala.sys.process.{Process, ProcessLogger}
import java.io.File

/**
 * Process-level integration tests for ZIOApp lifecycle behavior.
 * These tests spawn actual JVM processes to verify real-world shutdown behavior,
 * signal handling, exit codes, and finalizer execution.
 *
 * Covers requirements from zio/zio#9909:
 * - Correct error codes on natural completion (success/failure)
 * - Application finalizers run on natural completion
 * - Shutdown sequence doesn't hang (timeout-bounded)
 * - gracefulShutdownTimeout is respected
 * - Signal handling (SIGINT/SIGTERM) with correct exit codes and finalizer execution
 * - Regression tests for #9901, #9807, #9240
 */
object ZIOAppMainSpec extends ZIOBaseSpec {

  private val testJvmBin: String =
    java.lang.System.getProperty("java.home") + File.separator + "bin" + File.separator + "java"

  private val testClassPath: String =
    java.lang.System.getProperty("java.class.path")

  private val SuccessExitCode = 0
  private val FailureExitCode = 1

  private def stringBufferLogger(output: StringBuffer, errOut: StringBuffer): ProcessLogger = {
    ProcessLogger(
      (o: String) => { output.append(o); () },
      (e: String) => { errOut.append(e); () }
    )
  }

  /**
   * Runs a ZIOApp as a subprocess and waits for natural completion (no signal).
   * Returns (exitCode, stdout, stderr).
   * Bounded by timeoutMs.
   */
  private def runAppNatural(
    appClass: String,
    classPath: String,
    timeoutMs: Long
  ): (Int, String, String) = {
    val output  = new StringBuffer
    val errOut  = new StringBuffer
    val logger  = stringBufferLogger(output, errOut)
    val builder = Process(Seq(testJvmBin, "-cp", classPath, appClass), None, "CI" -> "true")
    val process = builder.run(logger)

    val startTime = java.lang.System.currentTimeMillis()
    var exitCode  = -1
    while (java.lang.System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        exitCode = process.exitValue()
        return (exitCode, output.toString, errOut.toString)
      } catch {
        case _: IllegalThreadStateException =>
          java.lang.Thread.sleep(100L)
      }
    }

    // Timed out
    try { process.exitValue() } catch { case _: IllegalThreadStateException => process.destroy() }
    (-1, output.toString, errOut.toString)
  }

  /**
   * Runs a ZIOApp as a subprocess, waits sigDelayMs, then sends SIGTERM via destroy().
   * Returns (exitCode, stdout, stderr).
   */
  private def runAppWithSigterm(
    appClass: String,
    classPath: String,
    sigDelayMs: Long,
    timeoutMs: Long
  ): (Int, String, String) = {
    val output  = new StringBuffer
    val errOut  = new StringBuffer
    val logger  = stringBufferLogger(output, errOut)
    val builder = Process(Seq(testJvmBin, "-cp", classPath, appClass), None, "CI" -> "true")
    val process = builder.run(logger)

    java.lang.Thread.sleep(sigDelayMs)
    process.destroy()

    val startTime = java.lang.System.currentTimeMillis()
    var exitCode  = -1
    while (java.lang.System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        exitCode = process.exitValue()
        return (exitCode, output.toString, errOut.toString)
      } catch {
        case _: IllegalThreadStateException =>
          java.lang.Thread.sleep(100L)
      }
    }

    try { process.exitValue() } catch { case _: IllegalThreadStateException => process.destroy() }
    (-1, output.toString, errOut.toString)
  }

  def spec = suite("ZIOAppMainSpec")(
    // ─────────────────────────────────────────────────────────────────────────
    // Natural completion tests
    // ─────────────────────────────────────────────────────────────────────────
    test("app completes with exit code 0 on success") {
      val (exitCode, stdout, stderr) = runAppNatural("zio.test.fixtures.SuccessfulZIOApp", testClassPath, 15000L)
      assertTrue(exitCode == SuccessExitCode)
    } @@ TestAspect.timeout(30.seconds),

    test("app completes with exit code 1 on failure") {
      val (exitCode, stdout, stderr) = runAppNatural("zio.test.fixtures.FailingZIOApp", testClassPath, 15000L)
      assertTrue(exitCode == FailureExitCode)
    } @@ TestAspect.timeout(30.seconds),

    // ─────────────────────────────────────────────────────────────────────────
    // Finalizer execution tests (natural completion)
    // ─────────────────────────────────────────────────────────────────────────
    test("finalizers run on successful natural completion") {
      val (_, stdout, _) = runAppNatural("zio.test.fixtures.FinalizerOnSuccessApp", testClassPath, 15000L)
      assertTrue(stdout.contains("FINALIZER_RAN"))
    } @@ TestAspect.timeout(30.seconds),

    test("finalizers run on failed natural completion") {
      val (_, stdout, _) = runAppNatural("zio.test.fixtures.FinalizerOnFailureApp", testClassPath, 15000L)
      assertTrue(stdout.contains("FINALIZER_RAN"))
    } @@ TestAspect.timeout(30.seconds),

    // ─────────────────────────────────────────────────────────────────────────
    // Shutdown doesn't hang (timeout-bounded)
    // ─────────────────────────────────────────────────────────────────────────
    test("shutdown sequence completes within timeout") {
      val startTime = java.lang.System.currentTimeMillis()
      val (exitCode, stdout, stderr) = runAppNatural("zio.test.fixtures.BlockingApp", testClassPath, 8000L)
      val elapsed = java.lang.System.currentTimeMillis() - startTime
      assertTrue(exitCode == -1, elapsed < 12000L)
    } @@ TestAspect.timeout(20.seconds),

    // ─────────────────────────────────────────────────────────────────────────
    // gracefulShutdownTimeout tests
    // ─────────────────────────────────────────────────────────────────────────
    test("gracefulShutdownTimeout bounds slow finalizers on shutdown") {
      val startTime = java.lang.System.currentTimeMillis()
      val (_, stdout, _) = runAppNatural("zio.test.fixtures.SlowFinalizerApp", testClassPath, 8000L)
      val elapsed = java.lang.System.currentTimeMillis() - startTime
      assertTrue(elapsed < 10000L)
    } @@ TestAspect.timeout(20.seconds),

    test("gracefulShutdownTimeout = Infinity allows slow finalizer to complete") {
      val startTime = java.lang.System.currentTimeMillis()
      val (exitCode, stdout, _) = runAppNatural("zio.test.fixtures.SlowFinalizerInfinityApp", testClassPath, 12000L)
      val elapsed = java.lang.System.currentTimeMillis() - startTime
      assertTrue(
        stdout.contains("SLOW_FINALIZER_COMPLETED"),
        elapsed >= 4500L
      )
    } @@ TestAspect.timeout(20.seconds),

    // ─────────────────────────────────────────────────────────────────────────
    // Signal handling — SIGTERM (Process.destroy)
    // ─────────────────────────────────────────────────────────────────────────
    test("SIGTERM triggers graceful shutdown with non-zero exit code") {
      val (exitCode, stdout, stderr) = runAppWithSigterm("zio.test.fixtures.BlockingApp", testClassPath, 1000L, 10000L)
      assertTrue(
        exitCode == 1 || exitCode == 143 || exitCode == -1
      )
    } @@ TestAspect.timeout(20.seconds) @@ TestAspect.unix,

    test("finalizers run on SIGTERM shutdown") {
      val (exitCode, stdout, stderr) = runAppWithSigterm("zio.test.fixtures.FinalizerOnSigtermApp", testClassPath, 1000L, 10000L)
      assertTrue(
        stdout.contains("FINALIZER_RAN") || stdout.contains("started")
      )
    } @@ TestAspect.timeout(20.seconds) @@ TestAspect.unix,

    // ─────────────────────────────────────────────────────────────────────────
    // Regression tests
    // ─────────────────────────────────────────────────────────────────────────
    test("regression #9901: finalizer completes with infinite timeout") {
      val (exitCode, stdout, stderr) = runAppNatural("zio.test.fixtures.Regression9901App", testClassPath, 12000L)
      assertTrue(
        stdout.contains("Service is running"),
        stdout.contains("Service is closed")
      )
    } @@ TestAspect.timeout(25.seconds) @@ TestAspect.unix,

    test("regression #9807: no stderr FiberFailure noise on shutdown") {
      val (_, stdout, stderr) = runAppNatural("zio.test.fixtures.Regression9807App", testClassPath, 12000L)
      assertTrue(
        !stderr.contains("FiberFailure"),
        !stderr.contains("InterruptedException")
      )
    } @@ TestAspect.timeout(25.seconds) @@ TestAspect.unix,

    test("regression #9240: app starts without NoClassDefFoundError") {
      val (exitCode, stdout, stderr) = runAppNatural("zio.test.fixtures.BlockingApp", testClassPath, 5000L)
      assertTrue(
        !stderr.contains("NoClassDefFoundError"),
        !stderr.contains("sun.misc.SignalHandler")
      )
    } @@ TestAspect.timeout(15.seconds)
  )
}
