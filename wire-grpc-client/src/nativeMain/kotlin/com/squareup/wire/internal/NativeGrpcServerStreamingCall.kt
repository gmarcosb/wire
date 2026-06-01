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

import com.squareup.wire.GrpcMethod
import com.squareup.wire.GrpcServerStreamingCall
import com.squareup.wire.MessageSource
import com.squareup.wire.NativeGrpcStreamingResponseHandler
import com.squareup.wire.WireNativeGrpcClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import okio.Timeout
import okio.IOException

internal class NativeGrpcServerStreamingCall<S : Any, R : Any>(
  private val grpcClient: WireNativeGrpcClient,
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

    val messageBytes = method.requestAdapter.encode(request)
    val url = "${grpcClient.baseUrl.url}/${method.path}"

    val responseChannel = Channel<R>(Channel.UNLIMITED)

    val handler = object : NativeGrpcStreamingResponseHandler {
      override fun onHeaders(metadata: Map<String, String>) {
        responseMetadata = metadata
      }

      override fun onMessage(message: ByteArray) {
        if (!canceled) {
          try {
            val decoded = method.responseAdapter.decode(message)
            responseChannel.trySend(decoded)
          } catch (e: Exception) {
            responseChannel.close(e)
          }
        }
      }

      override fun onFailure(error: Throwable) {
        responseChannel.close(error)
      }

      override fun onClosed() {
        responseChannel.close()
      }
    }

    scope.launch {
      try {
        grpcClient.transport.executeServerStreaming(
          url = url,
          requestMetadata = requestMetadata,
          requestMessage = messageBytes,
          timeout = timeout,
          responseHandler = handler
        )
      } catch (e: Exception) {
        responseChannel.close(e)
      }
    }

    return responseChannel
  }

  override fun executeBlocking(request: S): MessageSource<R> {
    throw UnsupportedOperationException("executeBlocking is not supported on Native")
  }

  override fun isExecuted(): Boolean = executed

  override fun clone(): GrpcServerStreamingCall<S, R> {
    val result = NativeGrpcServerStreamingCall(grpcClient, method)
    result.requestMetadata += this.requestMetadata
    return result
  }
}
