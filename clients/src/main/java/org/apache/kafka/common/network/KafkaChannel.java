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

package org.apache.kafka.common.network;


import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.security.Principal;

public class KafkaChannel {
    private final String id;
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    private NetworkReceive receive;
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    /**
     * 移除读事件键
     */
    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    /**
     * 关注读事件键
     */
    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    /**
     * 是否不可读
     * @return true 不可读 false 可读
     */
    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 设置发送，设置NIO写事件
     * @param send
     */
    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        this.send = send;
        //关注写事件键
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 从SocketChannel读取数据
     * @return
     * @throws IOException
     */
    public NetworkReceive read() throws IOException {
        //没有读取完全时，返回的是null,继续进行读取
        NetworkReceive result = null;
        //接收缓冲区，每次接收一个请求完毕置空
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }
        //从SocketChannel读取
        receive(receive);
        //完全读取完毕
        if (receive.complete()) {
            receive.payload().rewind();
            //读取完全返回
            result = receive;
            receive = null;
        }
        return result;
    }

    /**
     * 发送数据
     * @return 发送结果
     * @throws IOException IO异常
     */
    public Send write() throws IOException {
        Send result = null;
        //写入完成返回send，否则返回null
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    /**
     * 接收数据
     * @param receive
     * @return
     * @throws IOException
     */
    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    /**
     * 发送数据
     * @param send 字节发送缓冲区
     * @return 是否写完
     * @throws IOException IO异常
     */
    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        //如果数据写入完毕，移除关注写事件
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        //返回写入成功
        return send.completed();
    }

}
