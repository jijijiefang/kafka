/**
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
package kafka.log

import kafka.message._
import kafka.common._
import kafka.utils._
import kafka.server.{LogOffsetMetadata, FetchDataInfo}
import org.apache.kafka.common.errors.CorruptRecordException

import scala.math._
import java.io.{IOException, File}


 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * 日志的一段。每个段有两个组件：日志和索引。日志是包含实际消息的FileMessageSet。索引是从逻辑偏移映射到物理文件位置的偏移索引。
 * 每个段都有一个基本偏移量，该偏移量<=此段中任何消息的最小偏移量>任何前一段中的任何偏移量。
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The message set containing log entries 包含日志项的消息集
 * @param index The offset index 偏移索引文件
 * @param baseOffset A lower bound on the offsets in this segment 此段起始偏移量
 * @param indexIntervalBytes The approximate number of bytes between entries in the index 代表的是索引的粒度，即写入多少字节之后生成一条索引
 * @param time The time instance 时间实例
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet,
                 val index: OffsetIndex,
                 val baseOffset: Long,
                 val indexIntervalBytes: Int,
                 val rollJitterMs: Long,
                 time: Time) extends Logging {

  var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index 自上次在偏移量索引中添加项以来的字节数*/
  private var bytesSinceLastIndexEntry = 0

  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time, fileAlreadyExists: Boolean = false, initFileSize: Int = 0, preallocate: Boolean = false) =
    this(new FileMessageSet(file = Log.logFilename(dir, startOffset), fileAlreadyExists = fileAlreadyExists, initFileSize = initFileSize, preallocate = preallocate),
         new OffsetIndex(Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         rollJitterMs,
         time)

  /* Return the size in bytes of this log segment 返回此日志段的大小（以字节为单位）*/
  def size: Long = log.sizeInBytes()

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   * 附加以给定偏移量开头的给定消息。如果需要，在索引中添加一个条目。
   * It is assumed this method is being called from within a lock.
   * 假定此方法是从锁中调用的。
   * @param offset The first offset in the message set. 偏移量–消息集中的第一个偏移量
   * @param messages The messages to append.
   */
  @nonthreadsafe
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d".format(messages.sizeInBytes, offset, log.sizeInBytes()))
      // append an entry to the index (if needed)
      //在.log文件中每写入4096的数据，则在.index文件中写入一条稀疏索引
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        index.append(offset, log.sizeInBytes())
        this.bytesSinceLastIndexEntry = 0
      }
      // append the messages 追加消息
      log.append(messages)
      this.bytesSinceLastIndexEntry += messages.sizeInBytes
    }
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   * 查找偏移量>=请求偏移量的第一条消息的物理文件位置。
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index. lowerBound参数是一种优化，如果我们已经知道文件中的有效起始位置高于索引的最大下限，则可以使用它
   *
   * @param offset The offset we want to translate 我们要转换的偏移量
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index. 开始搜索的文件位置的下限。这纯粹是一种优化，如果省略，搜索将从偏移索引中的位置开始
   *
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria. 日志中存储最小偏移量>=请求偏移量的消息的位置，如果没有消息满足此条件，则为null。
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    //通过index索引信息定位到小于等于startOffset 的最近记录位置， 利用的是二分查找算法
    val mapping = index.lookup(offset)
    //从小于等于startOffset的最近记录位置开始往后读取数据，直到读取到偏移量为startOffset的消息
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   * 从第一个偏移量>=startOffset开始读取此段的消息集。如果指定了maxOffset，则消息集将包含不超过maxSize字节，并将在maxOffset之前结束。
   * @param startOffset A lower bound on the first offset to include in the message set we read 包含在我们读取的消息集中的第一个偏移量的下限
   * @param maxSize The maximum number of bytes to include in the message set we read 我们读取的消息集中包含的最大字节数
   * @param maxOffset An optional maximum offset for the message set we read 我们读取的消息集的可选最大偏移量
   * @param maxPosition The maximum position in the log segment that should be exposed for read 我们读取的消息集的可选最大偏移量
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset, 获取的数据和偏移量为>=startOffset的第一条消息的偏移量元数据，
   *         or null if the startOffset is larger than the largest offset in this log 如果startOffset大于此日志中的最大偏移量，则为null
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size): FetchDataInfo = {
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes // this may change, need to save a consistent copy 这可能会更改，需要保存一致的副本
    //通过startOffset找到位于log中具体的物理位置，以字节为单位
    val startPosition = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null 如果起始位置已离开日志的末尾，则返回null
    if(startPosition == null)
      return null
    //组装offset 元数据信息
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size 如果大小为零，仍然返回一个大小为零的日志段
    //如果设置了maxOffset ，则根据其具体值计算实际需要读取的字节数
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset 根据是否给了我们maxOffset，计算要读取的消息集的长度
    val length = maxOffset match {
      case None =>
        // no max offset, just read until the max position 无最大偏移量，仅读取至最大位置
        min((maxPosition - startPosition.position).toInt, maxSize)
      case Some(offset) =>
        // there is a max offset, translate it to a file position and use that to calculate the max read size;存在最大偏移量，将其转换为文件位置，并使用该位置计算最大读取大小；
        // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the 当分区的Leader更改时，新Leader的高水位线可能在短窗口内小于前一引线中的真实高水位线。
        // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an 在此窗口中，如果消费者获取新领导者的高水位线和日志结束偏移之间的偏移量，我们希望返回空响应。
        // offset between new leader's high watermark and the log end offset, we want to return an empty response.
        if(offset < startOffset)
          return FetchDataInfo(offsetMetadata, MessageSet.Empty)
        val mapping = translateOffset(offset, startPosition.position)
        val endPosition =
          if(mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file 最大偏移量超出日志的末尾，请使用文件的结尾
          else
            mapping.position
        min(min(maxPosition, endPosition) - startPosition.position, maxSize).toInt
    }
    //通过FileMessageSet提供的指定物理偏移量和长度的read方法读取相应的数据
    FetchDataInfo(offsetMetadata, log.read(startPosition.position, length))
  }

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
   *
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt.
   *
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    index.truncate()
    index.resize(index.maxIndexSize)
    var validBytes = 0
    var lastIndexEntry = 0
    val iter = log.iterator(maxMessageSize)
    try {
      while(iter.hasNext) {
        val entry = iter.next
        entry.message.ensureValid()
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                entry.offset
              case _ =>
                ByteBufferMessageSet.deepIterator(entry).next().offset
          }
          index.append(startOffset, validBytes)
          lastIndexEntry = validBytes
        }
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: CorruptRecordException =>
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes
    log.truncateTo(validBytes)
    index.trimToValidSize()
    truncated
  }

  override def toString() = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    val mapping = translateOffset(offset)
    if(mapping == null)
      return 0
    index.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    index.resize(index.maxIndexSize)
    val bytesTruncated = log.truncateTo(mapping.position)
    if(log.sizeInBytes == 0)
      created = time.milliseconds
    bytesSinceLastIndexEntry = 0
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if(ms == null) {
      baseOffset
    } else {
      ms.messageSet.lastOption match {
        case None => baseOffset
        case Some(last) => last.nextOffset
      }
    }
  }

  /**
   * Flush this log segment to disk 将此日志段刷新到磁盘
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      index.flush()
    }
  }

  /**
   * Change the suffix for the index and log file for this log segment
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {

    def kafkaStorageException(fileType: String, e: IOException) =
      new KafkaStorageException(s"Failed to change the $fileType file suffix from $oldSuffix to $newSuffix for log segment $baseOffset", e)

    try log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("log", e)
    }
    try index.renameTo(new File(CoreUtils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    catch {
      case e: IOException => throw kafkaStorageException("index", e)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(index.close)
    CoreUtils.swallow(log.close)
  }

  /**
   * Delete this log segment from the filesystem.
   * @throws KafkaStorageException if the delete fails.
   */
  def delete() {
    val deletedLog = log.delete()
    val deletedIndex = index.delete()
    if(!deletedLog && log.file.exists)
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
  }
}
