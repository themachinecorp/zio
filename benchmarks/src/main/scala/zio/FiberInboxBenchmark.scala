package zio

import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio.internal.FiberInbox

import java.util.concurrent.TimeUnit

/**
 * JMH benchmarks for FiberInbox.
 *
 * FiberInbox is a single-reader, multi-writer mailbox used by FiberRuntime.
 * These benchmarks measure:
 *   1. Single-writer, single-reader (the common case) 2. Multiple-writers,
 *      single-reader (concurrent adds) 3. Comparison with ConcurrentLinkedQueue
 *      (the baseline)
 */
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(value = 3)
class FiberInboxBenchmark {

  // Sentinel object used as a stand-in for FiberMessage (avoids private[zio] visibility)
  private val Sentinel: AnyRef = new Object

  private val totalOps      = 1_000_000
  private val WriterThreads = 4

  // ── Single-writer benchmarks ────────────────────────────────────────────────
  //
  // The fiber itself is the only writer. This is the most common case.

  @Benchmark
  def fiberInboxSingleWriter1Msg(): Int = {
    val inbox = FiberInbox()
    var i     = 0
    while (i < totalOps) {
      inbox.add(Sentinel)
      inbox.poll()
      i += 1
    }
    i
  }

  @Benchmark
  def fiberInboxSingleWriter4Msgs(): Int = {
    val inbox = FiberInbox()
    var i     = 0
    while (i < totalOps) {
      inbox.add(Sentinel)
      inbox.add(Sentinel)
      inbox.add(Sentinel)
      inbox.add(Sentinel)
      inbox.poll()
      inbox.poll()
      inbox.poll()
      inbox.poll()
      i += 1
    }
    i
  }

  @Benchmark
  def fiberInboxSingleWriterDrain(): Int = {
    val inbox = FiberInbox()
    var i     = 0
    while (i < totalOps) {
      inbox.add(Sentinel)
      inbox.add(Sentinel)
      inbox.add(Sentinel)
      inbox.add(Sentinel)
      inbox.drain()
      i += 1
    }
    i
  }

  // ── Multi-writer benchmarks ─────────────────────────────────────────────────
  //
  // Multiple threads add to the same inbox; one fiber polls.

  @Benchmark
  def fiberInboxMultiWriter4Threads(): Int = {
    val inbox   = FiberInbox()
    val threads = new Array[Thread](WriterThreads)
    var t       = 0
    while (t < WriterThreads) {
      threads(t) = new Thread {
        override def run(): Unit = {
          var i         = 0
          val perThread = totalOps / WriterThreads
          while (i < perThread) {
            inbox.add(Sentinel)
            i += 1
          }
        }
      }
      threads(t).start()
      t += 1
    }
    var totalPolled = 0
    while (totalPolled < totalOps) {
      val m = inbox.poll()
      if (m != null) totalPolled += 1
    }
    t = 0
    while (t < WriterThreads) {
      threads(t).join()
      t += 1
    }
    totalPolled
  }

  @Benchmark
  def fiberInboxMultiWriterDrain(): Int = {
    val inbox   = FiberInbox()
    val threads = new Array[Thread](WriterThreads)
    var t       = 0
    while (t < WriterThreads) {
      threads(t) = new Thread {
        override def run(): Unit = {
          var i         = 0
          val perThread = totalOps / WriterThreads
          while (i < perThread) {
            inbox.add(Sentinel)
            i += 1
          }
        }
      }
      threads(t).start()
      t += 1
    }
    var totalPolled   = 0
    val expectedTotal = totalOps
    while (totalPolled < expectedTotal) {
      val m = inbox.drain()
      if (m != null) totalPolled += 1
    }
    t = 0
    while (t < WriterThreads) {
      threads(t).join()
      t += 1
    }
    totalPolled
  }

  // ── Baseline: ConcurrentLinkedQueue ─────────────────────────────────────────

  @Benchmark
  def clqSingleWriter1Msg(): Int = {
    val q = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]()
    var i = 0
    while (i < totalOps) {
      q.add(Sentinel)
      q.poll()
      i += 1
    }
    i
  }

  @Benchmark
  def clqSingleWriter4Msgs(): Int = {
    val q = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]()
    var i = 0
    while (i < totalOps) {
      q.add(Sentinel)
      q.add(Sentinel)
      q.add(Sentinel)
      q.add(Sentinel)
      q.poll()
      q.poll()
      q.poll()
      q.poll()
      i += 1
    }
    i
  }

  @Benchmark
  def clqMultiWriter4Threads(): Int = {
    val q       = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]()
    val threads = new Array[Thread](WriterThreads)
    var t       = 0
    while (t < WriterThreads) {
      threads(t) = new Thread {
        override def run(): Unit = {
          var i         = 0
          val perThread = totalOps / WriterThreads
          while (i < perThread) {
            q.add(Sentinel)
            i += 1
          }
        }
      }
      threads(t).start()
      t += 1
    }
    var totalPolled = 0
    while (totalPolled < totalOps) {
      val m = q.poll()
      if (m != null) totalPolled += 1
    }
    t = 0
    while (t < WriterThreads) {
      threads(t).join()
      t += 1
    }
    totalPolled
  }
}
