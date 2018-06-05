package com.graylog.splunk.output.senders;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpDataHandler_3 extends SimpleChannelInboundHandler<NioDatagramChannel>{
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause)throws Exception{
        ctx.close();
        cause.printStackTrace();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NioDatagramChannel nioDatagramChannel) throws Exception {

    }
}
