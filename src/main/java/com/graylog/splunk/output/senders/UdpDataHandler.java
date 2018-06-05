package com.graylog.splunk.output.senders;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

public class UdpDataHandler extends SimpleChannelInboundHandler<DatagramPacket>{
//    @Override
//    public void messageReceived(ChannelHandlerContext channelHandlerContext,
//                                DatagramPacket datagramPacket) throws Exception {
//        String response = datagramPacket.content().toString(CharsetUtil.UTF_8);
//
//            System.out.println(response);
//            channelHandlerContext.close();
//    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause)throws Exception{
        ctx.close();
        cause.printStackTrace();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DatagramPacket datagramPacket) throws Exception {

    }
}
