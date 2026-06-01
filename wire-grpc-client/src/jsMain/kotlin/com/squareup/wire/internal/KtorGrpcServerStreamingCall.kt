/*
 * Copyright (C) 2026 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.wire.internal

import com.squareup.wire.GrpcException
import com.squareup.wire.GrpcMethod
import com.squareup.wire.GrpcServerStreamingCall
import com.squareup.wire.GrpcStatus
import com.squareup.wire.KtorGrpcClient
import com.squareup.wire.MessageSource
import io.ktor.client.request.header
import io.ktor.client.request.preparePost
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.core.readBytes
import io.ktor.utils.io.core.isEmpty
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import okio.Buffer
import okio.IOException
import okio.Timeout

internal class KtorGrpcServerStreamingCall<S : Any, R : Any>(
  private val grpcClient: KtorGrpcClient,
  override val method: GrpcMethod<S, R>,
) : GrpcServerStreamingCall<S, R> {
  override val timeout: Timeout = Timeout()
  override var requestMetadata: Map<String, String> = mapOf()
  override var responseMetadata: Map<String, String>? = null
    private set

  private var canceled = false
  private var executed = false

  override fun cancel() {
    canceled = true
  }

  override fun isCanceled(): Boolean = canceled

  override suspend fun executeIn(scope: CoroutineScope, request: S): ReceiveChannel<R> {
    check(!executed) { "already executed" }
    executed = true

    val requestBytes = Buffer()
    requestBytes.writeByte(0) // not compressed
    val messageBytes = method.requestAdapter.encode(request)
    requestBytes.writeInt(messageBytes.size)
    requestBytes.write(messageBytes)

    val url = "${grpcClient.baseUrl.url}/${method.path}"

    val responseChannel = Channel<R>(Channel.UNLIMITED)

    scope.launch {
      try {
        grpcClient.client.preparePost(url) {
          contentType(ContentType("application", "grpc-web+proto"))
          header("x-grpc-web", "1")
          for ((key, value) in requestMetadata) {
            header(key, value)
          }
          setBody(requestBytes.readByteArray())
        }.execute { response ->
          responseMetadata = response.headers.entries().associate { it.key to it.value.first() }

          if (response.status.value != 200) {
            throw IOException("HTTP ${response.status.value}")
          }

          val grpcStatus = response.headers["grpc-status"]?.toIntOrNull()
          if (grpcStatus != null && grpcStatus != 0) {
            val grpcMessage = response.headers["grpc-message"]
            throw GrpcException(GrpcStatus.get(grpcStatus), grpcMessage)
          }

          val channel = response.bodyAsChannel()

          while (!channel.isClosedForRead) {
            val flagPacket = channel.readRemaining(1)
            if (flagPacket.exhausted()) break
            val flag = flagPacket.readBytes()[0]

            val lenPacket = channel.readRemaining(4)
            if (lenPacket.exhausted()) break
            val lenBytes = lenPacket.readBytes()
            val length = ((lenBytes[0].toLong() and 0xffL) shl 24) or
                         ((lenBytes[1].toLong() and 0xffL) shl 16) or
                         ((lenBytes[2].toLong() and 0xffL) shl 8) or
                         (lenBytes[3].toLong() and 0xffL)

            val dataPacket = channel.readRemaining(length)
            val data = dataPacket.readBytes()

            if (flag.toInt() == 0x00) {
              val message = method.responseAdapter.decode(data)
              if (!canceled) {
                responseChannel.trySend(message)
              }
            } else if (flag.toInt() == 0x80) {
              val trailersStr = data.decodeToString()
              val trailers = trailersStr.split("\r\n").filter { it.isNotEmpty() }.map {
                val idx = it.indexOf(":")
                if (idx != -1) {
                  it.substring(0, idx).trim() to it.substring(idx + 1).trim()
                } else {
                  it to ""
                }
              }.toMap()

              val tStatus = trailers["grpc-status"]?.toIntOrNull()
              if (tStatus != null && tStatus != 0) {
                val tMessage = trailers["grpc-message"]
                throw GrpcException(GrpcStatus.get(tStatus), tMessage)
              }
            }
          }

          responseChannel.close()
        }
      } catch (e: Exception) {
        responseChannel.close(e)
      }
    }

    return responseChannel
  }

  override fun executeBlocking(request: S): MessageSource<R> {
    throw UnsupportedOperationException("executeBlocking is not supported in JS")
  }

  override fun isExecuted(): Boolean = executed

  override fun clone(): GrpcServerStreamingCall<S, R> {
    val result = KtorGrpcServerStreamingCall(grpcClient, method)
    result.requestMetadata += this.requestMetadata
    return result
  }
}
