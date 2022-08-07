package com.phoenix.kstore

import com.phoenix.kstore.utils.getLogger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.string.StringDecoder
import io.netty.handler.codec.string.StringEncoder
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler

class ClientServerHandler(private val kql: KQL): SimpleChannelInboundHandler<String>() {

    override fun channelRead0(ctx: ChannelHandlerContext?, msg: String?) {
        if (msg == null) {
            ctx?.writeAndFlush("ERR: msg is null")
            return
        }

        val response = kql.parse(msg)
        if (response is MessageResponse) {
            ctx?.writeAndFlush(response.message)
            return
        }

        val batchResponse = (response as OperationResponse).batchResponse ?: run {
            ctx?.writeAndFlush("ERR: batch response null")
            return
        }

        if (batchResponse.transactionInfo.returning.isNotEmpty()) {
            batchResponse.transactionInfo.returning.forEach { (k, v) ->
                ctx?.write("${k.array().toString(Charsets.UTF_8)}: ${v.toString(Charsets.UTF_8)}")
            }
            ctx?.flush()
        } else {
            ctx?.writeAndFlush(batchResponse.transactionInfo.status.name)
        }
    }

    @Deprecated("Deprecated in Java")
    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        cause?.printStackTrace()
        ctx?.close()
    }
}

class ClientServer(private val port: Int, node: Node) {

    companion object {
        val logger by getLogger()
    }

    private val kql = KQL(node)

    fun start() {
        val bossGroup = NioEventLoopGroup(1)
        val workerGroup = NioEventLoopGroup()

        try {
            val bootstrap = ServerBootstrap()
            bootstrap.apply {
                group(bossGroup, workerGroup)
                channel(NioServerSocketChannel::class.java)
                option(ChannelOption.SO_BACKLOG, 100)
                handler(LoggingHandler(LogLevel.INFO))
                childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        val pipeline = ch.pipeline()
                        pipeline.addLast(StringDecoder())
                        pipeline.addLast(StringEncoder())
                        pipeline.addLast(ClientServerHandler(kql))
                    }
                })
            }

            val channelFuture = bootstrap.bind(port).sync()
            logger.info("ClientServer Started")
            channelFuture.channel().closeFuture().sync()
        } finally {
            logger.info("ClientServer shutting down...")
            bossGroup.shutdownGracefully()
            workerGroup.shutdownGracefully()
        }
    }
}
