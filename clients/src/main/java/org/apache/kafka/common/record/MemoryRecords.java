/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * A {@link Records} implementation backed by a ByteBuffer.
 * 由 ByteBuffer 支持的Records实现
 */
public class MemoryRecords implements Records {

    private final static int WRITE_LIMIT_FOR_READABLE_ONLY = -1;

    // the compressor used for appends-only 仅用于追加的压缩器
    private final Compressor compressor;

    // the write limit for writable buffer, which may be smaller than the buffer capacity 可写缓冲区的写入限制，可能小于缓冲区容量
    private final int writeLimit;

    // the capacity of the initial buffer, which is only used for de-allocation of writable records 初始缓冲区的容量，仅用于可写记录的解除分配
    private final int initialCapacity;

    // the underlying buffer used for read; while the records are still writable it is null 用于读取的底层缓冲区；虽然记录仍然是可写的，但它是空的
    private ByteBuffer buffer;

    // indicate if the memory records is writable or not (i.e. used for appends or read-only) 指示内存记录是否可写（即用于追加或只读）
    private boolean writable;

    // Construct a writable memory records 构造可写的内存记录
    private MemoryRecords(ByteBuffer buffer, CompressionType type, boolean writable, int writeLimit) {
        this.writable = writable;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();
        if (this.writable) {
            this.buffer = null;
            this.compressor = new Compressor(buffer, type);
        } else {
            this.buffer = buffer;
            this.compressor = null;
        }
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type, int writeLimit) {
        return new MemoryRecords(buffer, type, true, writeLimit);
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type) {
        // use the buffer capacity as the default write limit 使用缓冲区容量作为默认写入限制
        return emptyRecords(buffer, type, buffer.capacity());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer, CompressionType.NONE, false, WRITE_LIMIT_FOR_READABLE_ONLY);
    }

    /**
     * Append the given record and offset to the buffer
     */
    public void append(long offset, Record record) {
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");

        int size = record.size();
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.put(record.buffer());
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        record.buffer().rewind();
    }

    /**
     * Append a new record and offset to the buffer
     * 将新记录和偏移量追加到缓冲区
     * @return crc of the record
     */
    public long append(long offset, long timestamp, byte[] key, byte[] value) {
        if (!writable)
            throw new IllegalStateException("Memory records is not writable");

        int size = Record.recordSize(key, value);
        compressor.putLong(offset);
        compressor.putInt(size);
        long crc = compressor.putRecord(timestamp, key, value);
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        return crc;
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     * 检查我们是否有空间容纳包含给定键/值对的新记录
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     * 注意，返回值是基于写入压缩器的字节的估计值，如果真的使用压缩，这可能不准确。发生这种情况时，以下追加可能会导致底层字节缓冲区流中的动态缓冲区重新分配。
     * There is an exceptional case when appending a single message whose size is larger than the batch size, the
     * capacity will be the message size which is larger than the write limit, i.e. the batch size. In this case
     * the checking should be based on the capacity of the initialized buffer rather than the write limit in order
     * to accept this single record.
     * 有一种例外情况，当附加一条大于批量大小的消息时，容量将是大于写入限制的消息大小，即批量大小。
     * 在这种情况下，检查应该基于初始化缓冲区的容量而不是写入限制，以便接受这个单条记录
     */
    public boolean hasRoomFor(byte[] key, byte[] value) {
        if (!this.writable)
            return false;

        return this.compressor.numRecordsWritten() == 0 ?
            this.initialCapacity >= Records.LOG_OVERHEAD + Record.recordSize(key, value) :
            this.writeLimit >= this.compressor.estimatedBytesWritten() + Records.LOG_OVERHEAD + Record.recordSize(key, value);
    }

    /**
     * 一个消息批次是否已经满了
     * @return boolean
     */
    public boolean isFull() {
        return !this.writable || this.writeLimit <= this.compressor.estimatedBytesWritten();
    }

    /**
     * Close this batch for no more appends
     * 关闭此批次不再追加
     */
    public void close() {
        if (writable) {
            // close the compressor to fill-in wrapper message metadata if necessary
            compressor.close();

            // flip the underlying buffer to be ready for reads
            buffer = compressor.buffer();
            buffer.flip();

            // reset the writable flag
            writable = false;
        }
    }

    /**
     * The size of this record set
     */
    public int sizeInBytes() {
        if (writable) {
            return compressor.buffer().position();
        } else {
            return buffer.limit();
        }
    }

    /**
     * The compression rate of this record set
     */
    public double compressionRate() {
        if (compressor == null)
            return 1.0;
        else
            return compressor.compressionRate();
    }

    /**
     * Return the capacity of the initial buffer, for writable records
     * it may be different from the current buffer's capacity
     */
    public int initialCapacity() {
        return this.initialCapacity;
    }

    /**
     * Get the byte buffer that backs this records instance for reading
     */
    public ByteBuffer buffer() {
        if (writable)
            throw new IllegalStateException("The memory records must not be writable any more before getting its underlying buffer");

        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        if (writable) {
            // flip on a duplicate buffer for reading
            return new RecordsIterator((ByteBuffer) this.buffer.duplicate().flip(), false);
        } else {
            // do not need to flip for non-writable buffer
            return new RecordsIterator(this.buffer.duplicate(), false);
        }
    }
    
    @Override
    public String toString() {
        Iterator<LogEntry> iter = iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
        }
        builder.append(']');
        return builder.toString();
    }

    /** Visible for testing */
    public boolean isWritable() {
        return writable;
    }

    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        private final ByteBuffer buffer;
        private final DataInputStream stream;
        private final CompressionType type;
        private final boolean shallow;
        private RecordsIterator innerIter;

        // The variables for inner iterator
        private final ArrayDeque<LogEntry> logEntries;
        private final long absoluteBaseOffset;

        public RecordsIterator(ByteBuffer buffer, boolean shallow) {
            this.type = CompressionType.NONE;
            this.buffer = buffer;
            this.shallow = shallow;
            this.stream = new DataInputStream(new ByteBufferInputStream(buffer));
            this.logEntries = null;
            this.absoluteBaseOffset = -1;
        }

        // Private constructor for inner iterator.
        private RecordsIterator(LogEntry entry) {
            this.type = entry.record().compressionType();
            this.buffer = entry.record().value();
            this.shallow = true;
            this.stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type, entry.record().magic());
            long wrapperRecordOffset = entry.offset();
            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset.
            if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                this.logEntries = new ArrayDeque<>();
                long wrapperRecordTimestamp = entry.record().timestamp();
                while (true) {
                    try {
                        LogEntry logEntry = getNextEntryFromStream();
                        Record recordWithTimestamp = new Record(logEntry.record().buffer(),
                                                                wrapperRecordTimestamp,
                                                                entry.record().timestampType());
                        logEntries.add(new LogEntry(logEntry.offset(), recordWithTimestamp));
                    } catch (EOFException e) {
                        break;
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }
                }
                this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().offset();
            } else {
                this.logEntries = null;
                this.absoluteBaseOffset = -1;
            }

        }

        /*
         * Read the next record from the buffer.
         * 
         * Note that in the compressed message set, each message value size is set as the size of the un-compressed
         * version of the message value, so when we do de-compression allocating an array of the specified size for
         * reading compressed value data is sufficient.
         */
        @Override
        protected LogEntry makeNext() {
            if (innerDone()) {
                try {
                    LogEntry entry = getNextEntry();
                    // No more record to return.
                    if (entry == null)
                        return allDone();

                    // Convert offset to absolute offset if needed.
                    if (absoluteBaseOffset >= 0) {
                        long absoluteOffset = absoluteBaseOffset + entry.offset();
                        entry = new LogEntry(absoluteOffset, entry.record());
                    }

                    // decide whether to go shallow or deep iteration if it is compressed
                    CompressionType compression = entry.record().compressionType();
                    if (compression == CompressionType.NONE || shallow) {
                        return entry;
                    } else {
                        // init the inner iterator with the value payload of the message,
                        // which will de-compress the payload to a set of messages;
                        // since we assume nested compression is not allowed, the deep iterator
                        // would not try to further decompress underlying messages
                        // There will be at least one element in the inner iterator, so we don't
                        // need to call hasNext() here.
                        innerIter = new RecordsIterator(entry);
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new KafkaException(e);
                }
            } else {
                return innerIter.next();
            }
        }

        private LogEntry getNextEntry() throws IOException {
            if (logEntries != null)
                return getNextEntryFromEntryList();
            else
                return getNextEntryFromStream();
        }

        private LogEntry getNextEntryFromEntryList() {
            return logEntries.isEmpty() ? null : logEntries.remove();
        }

        private LogEntry getNextEntryFromStream() throws IOException {
            // read the offset
            long offset = stream.readLong();
            // read record size
            int size = stream.readInt();
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);
            // read the record, if compression is used we cannot depend on size
            // and hence has to do extra copy
            ByteBuffer rec;
            if (type == CompressionType.NONE) {
                rec = buffer.slice();
                int newPos = buffer.position() + size;
                if (newPos > buffer.limit())
                    return null;
                buffer.position(newPos);
                rec.limit(size);
            } else {
                byte[] recordBuffer = new byte[size];
                stream.readFully(recordBuffer, 0, size);
                rec = ByteBuffer.wrap(recordBuffer);
            }
            return new LogEntry(offset, new Record(rec));
        }

        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }
    }
}
