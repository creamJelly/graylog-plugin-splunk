/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.graylog.splunk.output.senders;

import com.graylog.splunk.output.SplunkSenderThread;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDPSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(UDPSender.class);

    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s");

    private final String hostname;
    private final int port;
    private final String params;

    boolean initialized = false;

    protected final BlockingQueue<String> queue;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    public UDPSender(String hostname, int port, String params) {
        LOG.info("初始化UDPSender");
        this.hostname = hostname;
        this.port = port;
        this.params = params;

        /*
          * This internal queue shields us from causing OutputBufferProcessor
          * timeouts for a short time without risking memory overload or
          * loosing messages in case of temporary connection problems.
          *
          * TODO: Make configurable.
          */
        this.queue = new LinkedBlockingQueue<>(512);
    }

    private void createBootstrap(final EventLoopGroup workerGroup) {
        final Bootstrap bootstrap = new Bootstrap();
        final SplunkSenderThread senderThread = new SplunkSenderThread(queue);

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .remoteAddress(new InetSocketAddress(hostname, port))
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        LOG.info("初始化Channel");
                        ch.pipeline().addLast(new StringEncoder());

                        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                                // we only send data, never read on the socket
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                LOG.info("channelActive - 开始线程");
                                senderThread.start(ctx.channel());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                LOG.info("Channel disconnected.");
                                senderThread.stop();
                                scheduleReconnect(ctx.channel().eventLoop());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                LOG.error("Exception caught", cause);
                            }
                        });
                    }
                });
        Channel channel = bootstrap.bind(0).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOG.info("Connected.");
                } else {
                    LOG.error("Connection failed: {}", future.cause().getMessage());
                    scheduleReconnect(future.channel().eventLoop());
                }
            }
        }).channel();
        senderThread.start(channel);
    }

    protected void scheduleReconnect(final EventLoopGroup workerGroup) {
        workerGroup.schedule(new Runnable() {
            @Override
            public void run() {
                LOG.info("Starting reconnect!");
                createBootstrap(workerGroup);
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void initialize() {
        createBootstrap(workerGroup);

        initialized = true;
    }

    @Override
    public void stop() {
        workerGroup.shutdownGracefully();
    }

    @Override
    public void send(Message message) {
        LOG.info("进入send方法");
        StringBuilder splunkMessage = new StringBuilder();
        splunkMessage.append(message.getTimestamp().toString("yyyy/MM/dd-HH:mm:ss.SSS"))
                .append(" ")
                .append(noNewLines(message.getMessage()))
                .append(" original_source=").append(escape(message.getField(Message.FIELD_SOURCE)));

        for (Map.Entry<String, Object> field : message.getFields().entrySet()) {
            LOG.info("udpSend key = "+field.getKey());
            if (Message.RESERVED_FIELDS.contains(field.getKey()) || field.getKey().equals(Message.FIELD_STREAMS)) {
                continue;
            }

            splunkMessage.append(" ").append(field.getKey()).append("=").append(escape(field.getValue()));
        }

        splunkMessage.append("\r\n");

        LOG.debug("Sending message: {}", splunkMessage);
//
        try {
            queue.put(splunkMessage.toString());
        } catch (InterruptedException e) {
            LOG.warn("Interrupted. Message was most probably lost.");
        }
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private Object noNewLines(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            value = ((String) value).replace("\n", " ").replace("\r", " ");
        }

        return value;
    }

    private Object escape(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            Matcher matcher = WHITESPACE_PATTERN.matcher((String) value);
            if (matcher.find()) {
                value = "\"" + value + "\"";
            }
        }

        return noNewLines(value);
    }

}
