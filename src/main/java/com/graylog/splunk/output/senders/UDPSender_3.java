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
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

    boolean initialized = false;

    int times = 0;

    private HashMap<String, LinkedHashMap<String, String>> xmlMap;

    protected final BlockingQueue<DatagramPacket> queue;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    public UDPSender_3(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;

        /*
          * This internal queue shields us from causing OutputBufferProcessor
          * timeouts for a short time without risking memory overload or
          * loosing messages in case of temporary connection problems.
          *
          * TODO: Make configurable.
          */
        this.queue = new LinkedBlockingQueue<>(512);
        initParams();
    }

    /**
     * 初始化参数
     */
    private void initParams() {
        try {
            String path = File.separator + "home" + File.separator + "graylog_conf" + File.separator + "BI.xml";
            File file = new File(path);
//            LOG.info("file path = " + path);
            if (!file.exists()) {
                LOG.error("file not exist!  path = " + path);
                xmlMap = null;
                return;
            }

            if (file.isDirectory()) {
                LOG.error("need file, not dir , path = " + path);
                xmlMap = null;
                return;
            }


            xmlMap = new HashMap<String, LinkedHashMap<String, String>>();
            InputStream in = new FileInputStream(path);
            SAXReader reader = new SAXReader();
            Document doc = reader.read(in);
            Element root = doc.getRootElement();
            readNode(root, "");
        }
        catch (Exception e) {
            e.printStackTrace();
            xmlMap = null;
        }
    }

    private void readNode(Element root, String structName) {
        if (root == null) return;
        // 获取属性
        if (!structName.equals("") && structName.length() > 0) {
            LinkedHashMap<String, String> valueMap = xmlMap.get(structName);
            String value = "NULL";
            // 给版本号赋默认值
            if (true == root.attributeValue("name").equals("Format")) {
                value = "1.0.0";
            }
            String valueType = root.attributeValue("type").trim();
            if (valueType.equals("int")) {
                value = "0";
            }
            valueMap.put(root.attributeValue("name").toLowerCase(), value);
            xmlMap.put(structName, valueMap);
            LOG.info("structName = " + structName + " value = " + valueMap.toString());

        }
        // 获取他的子节点
        if (root.getName().equals("struct")) {
            LOG.info("!!!!  " + root.attributeValue("name"));
            LinkedHashMap<String, String> valueMap = new LinkedHashMap<String, String>();
            valueMap.put("FlowName", root.attributeValue("name"));
            xmlMap.put(root.attributeValue("name"), valueMap);
        }
        List<Element> childNodes = root.elements();
        for (Element e : childNodes) {
            if (root.getName().equals("struct")) {
                readNode(e, root.attributeValue("name"));
            }
            else {
                readNode(e, "");
            }
        }
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
        try {
            if (null == xmlMap) {
                LOG.error("failed while init xmlName, can not send message");
                return;
            }

            Object obj = message.getField("FlowName");
            if (null == obj) {
                LOG.error("message without FlowName, message = " + message.toString());
                return;
            }
            String msgFlowName = obj.toString();
            if (!xmlMap.containsKey(msgFlowName)) {
                LOG.error("FlowName not exist in paramsMap, FlowName = " + msgFlowName);
                return;
            }

            LinkedHashMap<String, String> valueMap = (LinkedHashMap<String, String>) xmlMap.get(msgFlowName).clone();
            // 给各字段赋值
            for(Map.Entry<String, Object> field : message.getFields().entrySet()) {
                // 所有tlog字段全都转成小写，然后进行匹配
                String keyName = field.getKey().toLowerCase();
                if (valueMap.containsKey(keyName)) {
                    String str = field.getValue().toString();
//                    LOG.info("put into : " + keyName + "    " + str);
                    // 替换文本中的 "|"
                    str = str.replaceAll("\\|", "_");
                    valueMap.put(keyName, str);
                }
            }

            StringBuilder tlogMessage = new StringBuilder();
            for(Map.Entry<String, String> entry : valueMap.entrySet()) {
                if (tlogMessage.length() > 0) {
                    tlogMessage.append("|");
                }
                tlogMessage.append(entry.getValue());
            }
            tlogMessage.append("\r\n");

            String resultStr = tlogMessage.toString();
            LOG.info("ready to make struct     " + resultStr);
            ByteBuf byteBuf = Unpooled.buffer(resultStr.getBytes("UTF-8").length);
            byteBuf.writeBytes(resultStr.getBytes("UTF-8"));
            DatagramPacket resultData = new DatagramPacket(byteBuf, new InetSocketAddress(this.hostname, this.port));
            queue.put(resultData);
        }
        catch (Exception e) {
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
