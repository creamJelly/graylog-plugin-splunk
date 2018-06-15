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
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.string.StringEncoder;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDPSender_3 implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(UDPSender_3.class);

    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s");

    private final String hostname;
    private final int port;
    private String params;

    boolean initialized = false;

    int times = 0;

    private LinkedHashMap<String, String> paramsMap;
    private String flowName;
    private boolean initFlag = false;

    protected final BlockingQueue<DatagramPacket> queue;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    public UDPSender_3(String hostname, int port, String params) {
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

    public void initParamsMap() {
        this.paramsMap = new LinkedHashMap<String, String>();
        String[] paramList = this.params.split(",");
        for(String param : paramList) {
            String[] innerStr = param.split("=");
            if (innerStr.length != 2) {
                this.initFlag = false;
                LOG.error("params format not right   , " + param);
                return;
            }
            this.paramsMap.put(innerStr[0].trim(), innerStr[1]);
            if (innerStr[0].equals("FlowName")) {
                this.flowName = innerStr[1];
            }
        }

        if (this.flowName.equals("") || this.flowName.length() <= 0) {
            this.initFlag = false;
            LOG.error("flowname not exist ,  flowName = " + this.flowName);
            return;
        }
        LOG.info("init success !  , params = " + this.params);
        this.initFlag = true;
    }

    protected void createBootstrap(final EventLoopGroup workerGroup) {
        final Bootstrap bootstrap = new Bootstrap();
        final SplunkSenderThread senderThread = new SplunkSenderThread(queue);

        bootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .remoteAddress(new InetSocketAddress(hostname, port))
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel ch) throws Exception {
                        ch.pipeline().addLast(new StringEncoder());

                        ch.pipeline().addLast(new SimpleChannelInboundHandler<NioDatagramChannel>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, NioDatagramChannel msg) throws Exception {
                              // we only send data, never read on the socket
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
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
        bootstrap.bind(0).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOG.info("Connected.");
                } else {
                    LOG.error("Connection failed: {}", future.cause().getMessage());
                    scheduleReconnect(future.channel().eventLoop());
                }
            }
        });
//        senderThread.start(channel);
    }

    private void scheduleReconnect(final EventLoopGroup workerGroup) {
        workerGroup.schedule(new Runnable() {
            @Override
            public void run() {
                times += 1;
                if (times <= 5) {
                    LOG.info("Starting reconnect!");
                    createBootstrap(workerGroup);
                }
                else {
                    LOG.info("reconnect over 5 times, stop!");
                }
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
        if (!this.initFlag) {
            LOG.error("failed while init paramsMap, stop in send func, params = " + this.params);
            return;
        }
        StringBuilder splunkMessage = new StringBuilder();
//        String flowName = "";
//        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
//        String[] paramList = this.params.split(",");
//        for(String param : paramList) {
//            String[] innerStr = param.split("=");
//            map.put(innerStr[0].trim(), innerStr[1]);
//            if (innerStr[0].equals("FlowName")) {
//                flowName = innerStr[1];
//            }
//        }
//
//        if (flowName.equals("") || flowName.length() <= 0) {
////            LOG.info("need flowName");
//            return;
//        }
        LinkedHashMap<String, String> map = (LinkedHashMap<String, String>)this.paramsMap.clone();

        // 给各字段赋值
        Boolean sendFlag = false;
        for(Map.Entry<String, Object> field : message.getFields().entrySet()) {
//            LOG.info(" in message      key = " + field.getKey() + "  value = " + field.getValue());
            if (field.getKey().equals("FlowName") && field.getValue().equals(map.get("FlowName"))) {
                sendFlag = true;
            }
            if (!field.getKey().equals("FlowName") && map.containsKey(field.getKey())) {
                map.put(field.getKey(), field.getValue().toString());
//                LOG.info("put value key = " + field.getKey() + " value = " + field.getValue().toString());
            }
        }

        if (!sendFlag) {
//            LOG.info("flowName not match, flowName = " + flowName);
            return;
        }

        for(Map.Entry<String, String> entry : map.entrySet()) {
            if (splunkMessage.length() > 0) {
                splunkMessage.append("|");
            }
            splunkMessage.append(entry.getValue());
        }
        splunkMessage.append("\r\n");

        try {
            String resultStr = splunkMessage.toString();
            LOG.info("ready to make struct     " + resultStr);
            ByteBuf byteBuf = Unpooled.buffer(resultStr.getBytes().length);
            byteBuf.writeBytes(resultStr.getBytes());
            DatagramPacket resultData = new DatagramPacket(byteBuf, new InetSocketAddress(this.hostname, this.port));
            queue.put(resultData);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
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
