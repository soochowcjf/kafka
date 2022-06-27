/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MultiRecordsSend;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * This class provides a way to build {@link Send} objects for network transmission
 * from generated {@link org.apache.kafka.common.protocol.ApiMessage} types without
 * allocating new space for "zero-copy" fields (see {@link #writeByteBuffer(ByteBuffer)}
 * and {@link #writeRecords(BaseRecords)}).
 *
 * See {@link org.apache.kafka.common.requests.EnvelopeRequest#toSend(RequestHeader)}
 * for example usage.
 */
public class SendBuilder implements Writable {
    private final ByteBuffer buffer;

    private final Queue<Send> sends = new ArrayDeque<>(1);
    private long sizeOfSends = 0;

    private final List<ByteBuffer> buffers = new ArrayList<>();
    private long sizeOfBuffers = 0;

    SendBuilder(int size) {
        // 新建一个byteBuffer
        this.buffer = ByteBuffer.allocate(size);
        // 标识起始位置
        this.buffer.mark();
    }

    @Override
    public void writeByte(byte val) {
        buffer.put(val);
    }

    @Override
    public void writeShort(short val) {
        buffer.putShort(val);
    }

    @Override
    public void writeInt(int val) {
        buffer.putInt(val);
    }

    @Override
    public void writeLong(long val) {
        buffer.putLong(val);
    }

    @Override
    public void writeDouble(double val) {
        buffer.putDouble(val);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        buffer.put(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buffer);
    }

    /**
     * Write a byte buffer. The reference to the underlying buffer will
     * be retained in the result of {@link #build()}.
     *
     * @param buf the buffer to write
     */
    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        flushPendingBuffer();
        addBuffer(buf.duplicate());
    }

    @Override
    public void writeVarint(int i) {
        ByteUtils.writeVarint(i, buffer);
    }

    @Override
    public void writeVarlong(long i) {
        ByteUtils.writeVarlong(i, buffer);
    }

    private void addBuffer(ByteBuffer buffer) {
        buffers.add(buffer);
        sizeOfBuffers += buffer.remaining();
    }

    private void addSend(Send send) {
        sends.add(send);
        sizeOfSends += send.size();
    }

    private void clearBuffers() {
        buffers.clear();
        sizeOfBuffers = 0;
    }

    /**
     * Write a record set. The underlying record data will be retained
     * in the result of {@link #build()}. See {@link BaseRecords#toSend()}.
     *
     * @param records the records to write
     */
    @Override
    public void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            // 固定字段的buffer
            flushPendingBuffer();
            // 零拷贝的buffer
            addBuffer(((MemoryRecords) records).buffer());
        } else if (records instanceof UnalignedMemoryRecords) {
            flushPendingBuffer();
            addBuffer(((UnalignedMemoryRecords) records).buffer());
        } else {
            flushPendingSend();
            addSend(records.toSend());
        }
    }

    private void flushPendingSend() {
        // 举个例子，如果是发送ProduceRequestData，由多个buffer组成的，消息头消息体的固定字段buffer+零拷贝的buffer组成的
        flushPendingBuffer();
        if (!buffers.isEmpty()) {
            ByteBuffer[] byteBufferArray = buffers.toArray(new ByteBuffer[0]);
            addSend(new ByteBufferSend(byteBufferArray, sizeOfBuffers));
            clearBuffers();
        }
    }

    private void flushPendingBuffer() {
        // 当前已经写入了消息头信息，先记录当前写到的position
        int latestPosition = buffer.position();
        // 恢复到起始位置
        buffer.reset();

        // 如果当前位置 > 起始位置，说明已经写入了消息
        if (latestPosition > buffer.position()) {
            // 截出来这一块buffer
            buffer.limit(latestPosition);
            addBuffer(buffer.slice());

            // 这是用于写后面的字段的
            buffer.position(latestPosition);
            // 限制后面写入的limit
            buffer.limit(buffer.capacity());
            // 标识当前的position
            buffer.mark();
        }
    }

    public Send build() {
        flushPendingSend();

        if (sends.size() == 1) {
            return sends.poll();
        } else {
            return new MultiRecordsSend(sends, sizeOfSends);
        }
    }

    public static Send buildRequestSend(
        RequestHeader header,
        Message apiRequest
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiRequest,
            header.apiVersion()
        );
    }

    public static Send buildResponseSend(
        ResponseHeader header,
        Message apiResponse,
        short apiVersion
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiResponse,
            apiVersion
        );
    }

    private static Send buildSend(
        Message header,
        short headerVersion,
        Message apiMessage,
        short apiVersion
    ) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        MessageSizeAccumulator messageSize = new MessageSizeAccumulator();
        header.addSize(messageSize, serializationCache, headerVersion);
        apiMessage.addSize(messageSize, serializationCache, apiVersion);

        // 消息体总长度 - 数据长度（这部分零拷贝）+ 头部长度域4个字节
        SendBuilder builder = new SendBuilder(messageSize.sizeExcludingZeroCopy() + 4);
        // 写入4字节的总长度域
        builder.writeInt(messageSize.totalSize());
        // 写入header
        header.write(builder, serializationCache, headerVersion);
        // 写入message
        apiMessage.write(builder, serializationCache, apiVersion);

        return builder.build();
    }

}
