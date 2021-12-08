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

import org.apache.kafka.common.utils.Utils

import scala.math._
import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.locks._
import kafka.utils._
import kafka.utils.CoreUtils.inLock
import kafka.common.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse: 将偏移量映射到特定日志段的物理文件位置的索引
 * that is it may not hold an entry for all messages in the log. 此索引可能是稀疏的：也就是说，它可能不包含日志中所有消息的条目
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 索引存储在一个文件中，该文件预先分配用于保存固定最大数量的8字节条目
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 索引支持查找此文件的内存映射。这些查找是使用简单的二进制搜索变量来定位小于或等于目标偏移量的最大偏移量的偏移量/位置对
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 索引文件可以通过两种方式打开：一种是作为允许追加的空的可变索引打开，另一种是作为以前已填充的不可变只读索引文件打开。makeReadOnly方法会将可变文件转换为不可变文件，并截断任何额外的字节。这是在翻转索引文件时完成的。
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 不会尝试对此文件的内容进行校验和，如果发生崩溃，则会重新生成该文件。
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 文件格式是一系列条目。物理格式是4字节的“相对”偏移量，以及具有该偏移量的消息的4字节文件位置。存储的偏移量相对于索引文件的基准偏移量。因此，例如，如果基准偏移量为50，则偏移量55将存储为5。以这种方式使用相对偏移量，让我们仅使用4个字节作为偏移量。
 * The frequency of entries is up to the user of this class.
 * 条目的频率由该类的用户决定。
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format. 所有外部API都从相对偏移量转换为完整偏移量，因此此类用户不会与内部存储格式交互。
 */
class OffsetIndex(@volatile private[this] var _file: File, val baseOffset: Long, val maxIndexSize: Int = -1) extends Logging {
  
  private val lock = new ReentrantLock
  
  /* initialize the memory mapping for this index 初始化此索引的内存映射*/
  @volatile
  private[this] var mmap: MappedByteBuffer = {
    val newlyCreated = _file.createNewFile()
    val raf = new RandomAccessFile(_file, "rw")
    try {
      /* pre-allocate the file if necessary 如有必要，预先分配文件*/
      if (newlyCreated) {
        if (maxIndexSize < 8)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        raf.setLength(roundToExactMultiple(maxIndexSize, 8))
      }

      /* memory-map the file 内存映射文件*/
      val len = raf.length()
      val idx = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, len)

      /* set the position in the index for the next entry 为下一个条目设置索引中的位置*/
      if (newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is all valid and set position to last entry 如果这是一个预先存在的索引，则假定它全部有效，并将位置设置为最后一个条目
        idx.position(roundToExactMultiple(idx.limit, 8))
      idx
    } finally {
      CoreUtils.swallow(raf.close())
    }
  }

  /* the number of eight-byte entries currently in the index 当前在索引中的八字节项的数目*/
  @volatile
  private[this] var _entries = mmap.position / 8

  /* The maximum number of eight-byte entries this index can hold 此索引可容纳的最大八字节项数*/
  @volatile
  private[this] var _maxEntries = mmap.limit / 8

  @volatile
  private[this] var _lastOffset = readLastEntry.offset
  
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
    .format(_file.getAbsolutePath, _maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position))

  /** The maximum number of entries this index can hold */
  def maxEntries: Int = _maxEntries

  /** The last offset in the index */
  def lastOffset: Long = _lastOffset

  /** The index file */
  def file: File = _file

  /**
   * The last entry in the index
   */
  def readLastEntry(): OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => OffsetPosition(baseOffset + relativeOffset(mmap, s - 1), physical(mmap, s - 1))
      }
    }
  }

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and its corresponding physical file position.
   * 找到小于或等于给定targetOffset的最大偏移量，并返回一对包含此偏移量及其相应的物理文件位置的偏移量
   * @param targetOffset The offset to look up. 要查找的偏移量
   * 
   * @return The offset found and the corresponding file position for this offset. 找到的偏移量以及该偏移量对应的文件位置
   * If the target offset is smaller than the least entry in the index (or the index is empty),
   * the pair (baseOffset, 0) is returned.
   * 如果目标偏移量小于索引中的最小项（或索引为空），则返回该对(baseOffset，0)
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, targetOffset)
      if(slot == -1)
        OffsetPosition(baseOffset, 0)
      else//返回偏移量和物理地址
        OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot))
      }
  }
  
  /**
   * Find the slot in which the largest offset less than or equal to the given
   * target offset is stored.
   * 查找存储小于或等于给定目标偏移量的最大偏移量的插槽
   * @param idx The index buffer 索引缓冲区
   * @param targetOffset The offset to look for 要查找的偏移量
   * 
   * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty 找到的插槽，如果索引中的最小项大于目标偏移量或索引为空，则为-1
   */
  private def indexSlotFor(idx: ByteBuffer, targetOffset: Long): Int = {
    // we only store the difference from the base offset so calculate that 我们只存储与基准偏移的差值，因此计算
    val relOffset = targetOffset - baseOffset
    
    // check if the index is empty 检查索引是否为空
    if (_entries == 0)
      return -1
    
    // check if the target offset is smaller than the least offset 检查目标偏移是否小于最小偏移
    if (relativeOffset(idx, 0) > relOffset)
      return -1
      
    // binary search for the entry 对条目进行二分搜索
    var lo = 0
    var hi = _entries - 1
    while (lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = relativeOffset(idx, mid)
      //找到返回
      if (found == relOffset)
        return mid
      //start增大
      else if (found < relOffset)
        lo = mid
      else {
        //end缩小
        hi = mid - 1
      }
    }
    lo
  }
  
  /* return the nth offset relative to the base offset 返回相对于基准偏移的第n个偏移*/
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8)
  
  /* return the nth physical position 返回物理地址*/
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4)
  
  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n, _entries))
      val idx = mmap.duplicate
      OffsetPosition(relativeOffset(idx, n), physical(idx, n))
    }
  }
  
  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   * 将给定偏移量/位置对的条目追加到索引中。此条目的偏移量必须大于所有后续条目的偏移量
   */
  def append(offset: Long, position: Int) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      if (_entries == 0 || offset > _lastOffset) {
        debug("Adding index entry %d => %d to %s.".format(offset, position, _file.getName))
        //offset是物理偏移量，baseOffset是当前文件起始物理偏移量
        mmap.putInt((offset - baseOffset).toInt)
        //.log文件占用byte数
        mmap.putInt(position)
        //条目数量+1
        _entries += 1
        _lastOffset = offset
        require(_entries * 8 == mmap.position, _entries + " entries but file position in index is " + mmap.position + ".")
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
          .format(offset, _entries, _lastOffset, _file.getAbsolutePath))
      }
    }
  }
  
  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries
  
  /**
   * Truncate the entire index, deleting all entries
   */
  def truncate() = truncateToEntries(0)
  
  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long) {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, offset)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries = 
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int) {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * 8)
      _lastOffset = readLastEntry.offset
    }
  }
  
  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize() {
    inLock(lock) {
      resize(_entries * 8)
    }
  }

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   */
  def resize(newSize: Int) {
    inLock(lock) {
      val raf = new RandomAccessFile(_file, "rw")
      val roundedNewSize = roundToExactMultiple(newSize, 8)
      val position = mmap.position
      
      /* Windows won't let us modify the file length while the file is mmapped :-( */
      if (Os.isWindows)
        forceUnmap(mmap)
      try {
        raf.setLength(roundedNewSize)
        mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
        _maxEntries = mmap.limit / 8
        mmap.position(position)
      } finally {
        CoreUtils.swallow(raf.close())
      }
    }
  }
  
  /**
   * Forcefully free the buffer's mmap. We do this only on windows.
   */
  private def forceUnmap(m: MappedByteBuffer) {
    try {
      if(m.isInstanceOf[sun.nio.ch.DirectBuffer])
        (m.asInstanceOf[sun.nio.ch.DirectBuffer]).cleaner().clean()
    } catch {
      case t: Throwable => warn("Error when freeing index buffer", t)
    }
  }
  
  /**
   * Flush the data in the index to disk
   */
  def flush() {
    inLock(lock) {
      mmap.force()
    }
  }
  
  /**
   * Delete this index file
   */
  def delete(): Boolean = {
    info("Deleting index " + _file.getAbsolutePath)
    if (Os.isWindows)
      CoreUtils.swallow(forceUnmap(mmap))
    _file.delete()
  }
  
  /** The number of entries in this index */
  def entries = _entries
  
  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes() = 8 * _entries
  
  /** Close the index */
  def close() {
    trimToValidSize()
  }
  
  /**
   * Rename the file that backs this offset index
   * @throws IOException if rename fails
   */
  def renameTo(f: File) {
    try Utils.atomicMoveWithFallback(_file.toPath, f.toPath)
    finally _file = f
  }
  
  /**
   * Do a basic sanity check on this index to detect obvious problems
   * @throws IllegalArgumentException if any problems are found
   */
  def sanityCheck() {
    require(_entries == 0 || lastOffset > baseOffset,
            "Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
            .format(_file.getAbsolutePath, lastOffset, baseOffset))
    val len = _file.length()
    require(len % 8 == 0,
            "Index file " + _file.getName + " is corrupt, found " + len +
            " bytes which is not positive or not a multiple of 8.")
  }
  
  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundToExactMultiple(67, 8) == 64
   */
  private def roundToExactMultiple(number: Int, factor: Int) = factor * (number / factor)
  
  /**
   * Execute the given function in a lock only if we are running on windows. We do this 
   * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  private def maybeLock[T](lock: Lock)(fun: => T): T = {
    if(Os.isWindows)
      lock.lock()
    try {
      fun
    } finally {
      if(Os.isWindows)
        lock.unlock()
    }
  }
}
