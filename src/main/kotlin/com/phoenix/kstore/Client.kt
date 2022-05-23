package com.phoenix.kstore

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.StringDecoder
import io.netty.handler.codec.string.StringEncoder

class KStoreClientHandler: SimpleChannelInboundHandler<String>() {

    override fun channelRead0(ctx: ChannelHandlerContext?, msg: String?) {
        println(msg)
    }
}

class KStoreClient: CliktCommand() {

    val host: String by option(help = "Server hostname").default("127.0.0.1")
    val port: Int by option(help = "Server port").int().default(4000)

    override fun run() {
        val group = NioEventLoopGroup()
        try {
            val bootstrap = Bootstrap()
            bootstrap.apply {
                group(group)
                channel(NioSocketChannel::class.java)
                option(ChannelOption.TCP_NODELAY, true)
                handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        val pipeline = ch.pipeline()
                        pipeline.addLast(StringDecoder())
                        pipeline.addLast(StringEncoder())
                        pipeline.addLast(KStoreClientHandler())
                    }
                })
            }

            val channelFuture = bootstrap.connect(host, port).sync()

            while (true) {
                val query = prompt("query") ?: continue
                if (query == "q") break

                val channel = channelFuture.sync().channel()
                channel.writeAndFlush(query)
            }

            channelFuture.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully()
        }
    }
}

fun main(args: Array<String>) = KStoreClient().main(args)
