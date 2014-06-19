/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.{SizeEstimator, Utils}

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean, var dropping: Boolean = false)

/**
 * Stores blocks in memory, either as ArrayBuffers of deserialized Java objects or as
 * serialized ByteBuffers.
 *
 * When one thread is trying to add a block, first it will select blocks to be dropped
 * and mark them as dropping(the selection will skip blocks marked as dropping). The
 * selection and marking are done within a select lock which ensures each thread will select
 * different to-be-dropped blocks. Then the thread will do the dropping without any lock and
 * remove them from memory store after dropping finished. If exception happens, it will
 * reset the dropping flag of its to-be-dropped blocks so that other thread can re-select
 * and re-drop them.
 */
private class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)
  @volatile private var currentMemory = 0L
  // Object used to ensure that only one thread is putting blocks and if necessary, dropping
  // blocks from the memory store.
  private val selectLock = new Object()

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytes = _bytes.duplicate()
    bytes.rewind()
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      tryToPut(blockId, elements, sizeEstimate, true)
      PutResult(sizeEstimate, Left(values.toIterator))
    } else {
      tryToPut(blockId, bytes, bytes.limit, false)
      PutResult(bytes.limit(), Right(bytes.duplicate()))
    }
  }

  override def putValues(
      blockId: BlockId,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      val putAttempt = tryToPut(blockId, values, sizeEstimate, deserialized = true)
      PutResult(sizeEstimate, Left(values.iterator), putAttempt.droppedBlocks)
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putValues(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    val valueEntries = new ArrayBuffer[Any]()
    valueEntries ++= values
    putValues(blockId, valueEntries, level, returnValues)
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[ArrayBuffer[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[ArrayBuffer[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    entries.synchronized {
      val entry = entries.remove(blockId)
      if (entry != null) {
        currentMemory -= entry.size
        logInfo(s"Block $blockId of size ${entry.size} dropped from memory (free $freeMemory)")
        true
      } else {
        false
      }
    }
  }

  override def clear() {
    entries.synchronized {
      entries.clear()
      currentMemory = 0
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an ArrayBuffer if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated)
   * size must also be passed by the caller.
   *
   * Return whether put was successful, along with the blocks dropped in the process.
   */
  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {

    var putSuccess = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val result = findToBeDroppedBlocks(blockId, size)
    if (result.isDefined) {
      val toBeDroppedBlocks = result.get
      var droppingDoneCount = 0
      try {
        toBeDroppedBlocks.foreach { block =>
          val droppedBlockStatus = blockManager.dropFromMemory(block.id, block.data)
          droppedBlockStatus.foreach { status => droppedBlocks += blockId -> status }
          droppingDoneCount += 1
        }
      } finally {
        if (droppingDoneCount < toBeDroppedBlocks.size) {
          toBeDroppedBlocks.drop(droppingDoneCount).foreach { block =>
            entries.synchronized{
              val entry = entries.get(block.id)
              if (entry != null) entry.dropping = false
            }
          }
        }
      }

      val entry = new MemoryEntry(value, size, deserialized)
      entries.synchronized {
        entries.put(blockId, entry)
        currentMemory += size
      }
      val valuesOrBytes = if (deserialized) "values" else "bytes"
      logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
        blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
      putSuccess = true
    } else {
      // Tell the block manager that we couldn't put it in memory so that it can drop it to
      // disk if the block allows disk storage.
      val data = if (deserialized) {
        Left(value.asInstanceOf[ArrayBuffer[Any]])
      } else {
        Right(value.asInstanceOf[ByteBuffer].duplicate())
      }
      val droppedBlockStatus = blockManager.dropFromMemory(blockId, data)
      droppedBlockStatus.foreach { status => droppedBlocks += blockId -> status }
    }
    ResultWithDroppedBlocks(putSuccess, droppedBlocks)
  }

  /**
   * Try to select some blocks which will free up a given amount of space to store a
   * particular block, but can fail if either the block is bigger than our memory or
   * it would require selecting another block from the same RDD (which leads to a wasteful
   * cyclic replacement pattern for RDDs that don't fit into memory that we want to avoid).
   *
   * Return None if there is no enough free space, or Some[List[ToBeDroppedBlock]] if space is
   * enough and tell the caller which blocks need to be dropped
   */
  private def findToBeDroppedBlocks(blockIdToAdd: BlockId, space: Long): Option[Seq[ToBeDroppedBlock]] = {
    logInfo(s"ensureFreeSpace($space) called with curMem=$currentMemory, maxMem=$maxMemory")

    val toBeDroppedBlocks = new ArrayBuffer[ToBeDroppedBlock]

    if (space > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      return None
    }

    if (maxMemory - currentMemory < space) {
      val rddToAdd = getRddId(blockIdToAdd)
      val selectedBlocks = new ArrayBuffer[BlockId]()
      var selectedMemory = 0L

      // This lock ensures that the selection and marking for the to-be-dropped blocks
      // is done by only one thread at a time. Otherwise if one thread has selected some
      // blocks and going to mark them as dropping, another thread may select the same blocks
      // and mark them twice, which leads to incorrect calculation of free space.
      selectLock.synchronized {

        // This is synchronized to ensure that the set of entries is not changed
        // (because of getValue or getBytes) while traversing the iterator, as that
        // can lead to exceptions.
        entries.synchronized {
          val iterator = entries.entrySet().iterator()
          while (maxMemory - (currentMemory - selectedMemory) < space && iterator.hasNext) {
            val pair = iterator.next()
            val entry = pair.getValue
            if (!entry.dropping) {
              val blockId = pair.getKey
              if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
                selectedBlocks += blockId
                selectedMemory += entry.size
              }
            }
          }
        }
      }

      if (maxMemory - (currentMemory - selectedMemory) >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          entries.synchronized {
            val entry = entries.get(blockId)

            // Currently we only remove block when drop it from memory, and each thread
            // will select different blocks to drop,so it won't happen that a selected
            // block has been removed. However the check is still here for future safety.
            if (entry != null) {
              val data = if (entry.deserialized) {
                Left(entry.value.asInstanceOf[ArrayBuffer[Any]])
              } else {
                Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
              }
              toBeDroppedBlocks += ToBeDroppedBlock(blockId, data)
              entry.dropping = true
            }
          }
        }
      } else {
        logInfo(s"Will not store $blockIdToAdd as it would require dropping another block " +
          "from the same RDD")
        return None
      }
    }
    Some(toBeDroppedBlocks)
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }
}

private case class ToBeDroppedBlock(id: BlockId, data: Either[ArrayBuffer[Any], ByteBuffer])

private case class ResultWithDroppedBlocks(
    success: Boolean,
    droppedBlocks: Seq[(BlockId, BlockStatus)])
