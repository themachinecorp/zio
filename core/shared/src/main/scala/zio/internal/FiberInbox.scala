/*
 * Copyright 2024 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or or in writing software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.internal

import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * A highly-optimized single-reader, multi-writer mailbox for the fiber
 * run-loop.
 *
 * ==Design==
 *
 * The inbox holds up to [[RingBufferSize]] messages in a fixed 4-slot ring
 * buffer. This handles the common case (1–4 messages) with zero allocation per
 * message. When the ring buffer is full, messages spill over to a
 * [[ConcurrentLinkedQueue]].
 *
 * The reader is ALWAYS the fiber itself (single-threaded), so no
 * synchronization is needed for read operations. Writers use lock-free CAS on
 * the shared write counter.
 *
 * ==Key Properties==
 *
 *   - Single reader: [[poll]] and [[isEmpty]] are called only by the owning
 *     fiber
 *   - Multiple writers: any thread can call [[add]] concurrently using CAS
 *   - Small typical size: the 4-slot ring buffer handles 1–4 messages with no
 *     allocation
 *   - CLQ fallback: overflow goes to a lock-free linked queue (rare in
 *     practice)
 *   - `isEmpty` may spuriously return `false` (see [[isEmptyApprox]]); always
 *     follow up with `poll() == null` to confirm
 *
 * ==Ring Buffer Mechanics==
 *
 * Each slot has a sequence number. The writer writes to the slot matching its
 * current sequence, then advances `_writeSeq` to publish. The reader drains
 * slots whose sequence number exceeds `_readSeq`. When a batch is drained,
 * freed slots are stamped with `_readSeq + RingBufferSize` so they reuse
 * cleanly on the next lap.
 */
private[zio] final class FiberInbox[A] private () {

  // The ring buffer slots (fixed size, no allocation per message)
  private val ringBuffer: Array[FiberMessage] = new Array[FiberMessage](RingBufferSize)

  // Per-slot sequence numbers: initially 0, written by both reader and writers.
  // Writers check that slotSeq == their expected seq before writing.
  // Reader writes `_readSeq + RingBufferSize` to reclaim a slot.
  private val slotSeq: Array[Int] = new Array[Int](RingBufferSize)

  // Write sequence: incremented by each writer (CAS). Used to pick a slot.
  // Writers atomically claim a slot by CAS-ing this value.
  private val _writeSeq: AtomicInteger = new AtomicInteger(0)

  // Read sequence: written ONLY by the fiber (single reader).
  // No synchronization needed.
  @volatile private var _readSeq: Int = 0

  // Overflow queue: used when the ring buffer is contended/pinned.
  // ConcurrentLinkedQueue is lock-free and allocation-free on the offer path.
  private val overflow: ConcurrentLinkedQueue[FiberMessage] = new ConcurrentLinkedQueue[FiberMessage]()

  /**
   * Adds a message to the inbox. Can be called by any thread (multi-writer
   * safe).
   *
   * @param m
   *   the message to enqueue
   */
  private[zio] def add(m: FiberMessage): Unit = {
    val slot = tryClaimSlot()
    if (slot < 0) {
      // Ring buffer exhausted (very rare); fall back to the concurrent queue.
      overflow.add(m)
    } else {
      ringBuffer(slot) = m
      // Publish: advance _writeSeq so the reader sees the message.
      // We add RingBufferSize so the sequence number wraps cleanly.
      _writeSeq.addAndGet(RingBufferSize)
    }
  }

  /**
   * Atomically claims a slot in the ring buffer for the current writer.
   *
   * @return
   *   the slot index (0–RingBufferSize-1), or -1 if the ring is contended
   */
  @inline private def tryClaimSlot(): Int = {
    var seq  = _writeSeq.get()
    var spin = 0

    while (spin < SpinLimit) {
      val idx      = seq & RingBufferMask
      val expected = seq

      // Claim the slot if it's at the expected sequence
      if (slotSeq(idx) == expected) {
        slotSeq(idx) = expected + 1
        return idx
      }

      // Slot already claimed by another writer or not yet ready.
      // Try to advance _writeSeq so we get a fresh slot on retry.
      val updated = seq + 1
      if (_writeSeq.compareAndSet(seq, updated)) {
        seq = updated
      } else {
        seq = _writeSeq.get()
      }
      spin += 1
    }

    // Ring buffer is pinned under heavy contention; let the caller fall back.
    -1
  }

  /**
   * Drains ALL messages from the inbox, returning the last one polled.
   *
   * Called ONLY by the fiber (single reader). No synchronization needed.
   *
   * @return
   *   the last message, or null if the inbox was empty
   */
  private[zio] def drain(): FiberMessage = {
    var msg: FiberMessage = null.asInstanceOf[FiberMessage]
    var m: FiberMessage   = null

    // Drain the ring buffer
    var seq = _readSeq
    var idx = seq & RingBufferMask
    while (slotSeq(idx) > seq) {
      m = ringBuffer(idx)
      ringBuffer(idx) = null
      slotSeq(idx) = seq + RingBufferSize // reclaim: stamp for next lap
      _readSeq = seq + 1
      seq += 1
      msg = m
      idx = seq & RingBufferMask
    }

    // Drain the overflow queue
    m = overflow.poll()
    while (m ne null) {
      msg = m
      m = overflow.poll()
    }

    msg
  }

  /**
   * Polls for a single message from the inbox.
   *
   * Called ONLY by the fiber (single reader).
   *
   * @return
   *   a message, or null if the inbox is empty
   */
  private[zio] def poll(): FiberMessage = {
    // Try ring buffer first (hot path)
    val seq = _readSeq
    val idx = seq & RingBufferMask
    if (slotSeq(idx) > seq) {
      val m = ringBuffer(idx)
      ringBuffer(idx) = null
      slotSeq(idx) = seq + RingBufferSize
      _readSeq = seq + 1
      return m
    }

    // Try overflow queue
    val m = overflow.poll()
    if (m ne null) return m

    null.asInstanceOf[FiberMessage]
  }

  /**
   * Approximate isEmpty check.
   *
   * Returns `false` if the inbox is definitely NOT empty. Returns `true` if the
   * inbox MAY be empty (may be a false positive).
   *
   * This is safe to use in the fiber drain loop to decide whether to spin up
   * the executor: a false positive only causes an unnecessary yield, not a bug.
   *
   * This is called by the fiber (single reader) so no synchronization is
   * needed.
   */
  private[zio] def isEmptyApprox: Boolean = {
    val seq = _readSeq
    val idx = seq & RingBufferMask
    if (slotSeq(idx) > seq) return false // definitely not empty
    !overflow.isEmpty
  }

  /**
   * Precise isEmpty check. May be slightly slower than [[isEmptyApprox]].
   *
   * Called by FiberRuntime to determine whether to record the fiber as done.
   */
  private[zio] def isEmpty: Boolean = {
    val seq = _readSeq
    val idx = seq & RingBufferMask
    if (slotSeq(idx) > seq) return false
    !overflow.isEmpty
  }

  // ── Constants ──────────────────────────────────────────────────────────────

  /** Number of slots in the ring buffer. Must be a power of 2. */
  private[this] val RingBufferSize: Int = 4

  /** Mask for modulo RingBufferSize (since RingBufferSize is a power of 2). */
  private[this] val RingBufferMask: Int = RingBufferSize - 1

  /**
   * Maximum spin iterations before giving up on the ring buffer. With 4 slots
   * and a spin limit of 8, we handle burst contention gracefully.
   */
  private[this] val SpinLimit: Int = 8
}

/**
 * Factory for [[FiberInbox]].
 */
private[zio] object FiberInbox {
  def apply(): FiberInbox = new FiberInbox()
}
