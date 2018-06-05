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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDPSender_2 implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(UDPSender_2.class);

    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s");

    private final String hostname;
    private final int port;
    private final String params;

    boolean initialized = false;

    protected final BlockingQueue<String> queue;

    private static int times = 0;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private Channel channel;

    public UDPSender_2(String hostname, int port, String params) {
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

    protected void createBootstrap(final EventLoopGroup workerGroup){
        try {
            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup).channel(NioDatagramChannel.class)
                    .remoteAddress(this.hostname, this.port)
                    .handler(new UdpDataHandler());
            this.channel = bootstrap.bind(0).sync().channel();
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
            scheduleReconnect(workerGroup);
        }
    }

    protected void scheduleReconnect(final EventLoopGroup workerGroup) {
        workerGroup.schedule(new Runnable() {
            @Override
            public void run() {
                LOG.info("Starting reconnect!");
                times += 1;
                if (times <= 5) {
                    createBootstrap(workerGroup);
                }
                else {
                    LOG.error("重连超过五次，不再重连");
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
            ByteBuf byteBuf =  Unpooled.buffer(message.getMessage().getBytes().length);
            byteBuf.writeBytes(message.getMessage().getBytes());

            this.channel.writeAndFlush(
                    new DatagramPacket(
                            byteBuf,
                            new InetSocketAddress(
                                    this.hostname,this.port
                            )));
        }catch (Exception e){
            LOG.error(e.getMessage());
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
