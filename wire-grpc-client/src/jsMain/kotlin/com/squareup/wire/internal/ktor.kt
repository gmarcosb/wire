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

import com.squareup.wire.GrpcCall
import com.squareup.wire.GrpcException
import com.squareup.wire.GrpcMethod
import com.squareup.wire.GrpcStatus
import com.squareup.wire.KtorGrpcClient
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.readBytes
import io.ktor.client.statement.request
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import okio.Buffer
import okio.IOException
import okio.Timeout

internal class KtorGrpcCall<S : Any, R : Any>(
  private val grpcClient: KtorGrpcClient,
  override val method: GrpcMethod<S, R>,
) : GrpcCall<S, R> {
  override val timeout: Timeout = Timeout()
  override var requestMetadata: Map<String, String> = mapOf()
  override var responseMetadata: Map<String, String>? = null
    private set

  private var canceled = false
  private var executed = false

  override fun cancel() {
    canceled = true
    // Need cancellation token support in Ktor for actual cancellation of in-flight requests.
  }

  override fun isCanceled(): Boolean = canceled

  override suspend fun execute(request: S): R {
    check(!executed) { "already executed" }
    executed = true

    val requestBytes = Buffer()
    requestBytes.writeByte(0) // not compressed
    val messageBytes = method.requestAdapter.encode(request)
    requestBytes.writeInt(messageBytes.size)
    requestBytes.write(messageBytes)

    val url = "${grpcClient.baseUrl.url}/${method.path}"

    val response = grpcClient.client.post(url) {
      contentType(ContentType("application", "grpc-web+proto"))
      header("x-grpc-web", "1")
      for ((key, value) in requestMetadata) {
        header(key, value)
      }
      setBody(requestBytes.readByteArray())
    }

    responseMetadata = response.headers.entries().associate { it.key to it.value.first() }

    if (response.status.value != 200) {
      throw IOException("HTTP ${response.status.value}")
    }

    val grpcStatus = response.headers["grpc-status"]?.toIntOrNull()
    if (grpcStatus != null && grpcStatus != 0) {
      val grpcMessage = response.headers["grpc-message"]
      throw GrpcException(GrpcStatus.get(grpcStatus), grpcMessage)
    }

    val responseBytes = response.readBytes()
    val buffer = Buffer().write(responseBytes)

    if (buffer.exhausted()) {
      val exception = IOException("expected 1 message but got none")
      throw exception
    }

    val compressedFlag = buffer.readByte()
    if (compressedFlag.toInt() != 0) {
      throw IOException("compressed flag must be 0")
    }
    val messageLength = buffer.readInt().toLong() and 0xffffffffL
    val encodedMessage = buffer.readByteArray(messageLength)

    // Check for trailing trailers in grpc-web
    while (!buffer.exhausted()) {
      val flag = buffer.readByte()
      val len = buffer.readInt().toLong() and 0xffffffffL
      val data = buffer.readByteArray(len)
      if (flag.toInt() == 0x80) {
        // It's a trailer
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

    return method.responseAdapter.decode(encodedMessage)
  }

  override fun executeBlocking(request: S): R = throw UnsupportedOperationException("executeBlocking is not supported in JS")

  override fun enqueue(request: S, callback: GrpcCall.Callback<S, R>) {
    check(!executed) { "already executed" }
    executed = true

    GlobalScope.launch {
      try {
        val result = execute(request)
        callback.onSuccess(this@KtorGrpcCall, result)
      } catch (e: Exception) {
        callback.onFailure(this@KtorGrpcCall, IOException(e.message))
      }
    }
  }

  override fun isExecuted(): Boolean = executed

  override fun clone(): GrpcCall<S, R> {
    val result = KtorGrpcCall(grpcClient, method)
    result.requestMetadata += this.requestMetadata
    return result
  }
}
