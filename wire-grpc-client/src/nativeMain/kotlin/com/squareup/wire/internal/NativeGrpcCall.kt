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
import com.squareup.wire.WireNativeGrpcClient
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import okio.IOException
import okio.Timeout

internal class NativeGrpcCall<S : Any, R : Any>(
  private val grpcClient: WireNativeGrpcClient,
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
  }

  override fun isCanceled(): Boolean = canceled

  override suspend fun execute(request: S): R {
    check(!executed) { "already executed" }
    executed = true

    val messageBytes = method.requestAdapter.encode(request)
    val url = "${grpcClient.baseUrl.url}/${method.path}"

    val response = grpcClient.transport.execute(
      url = url,
      requestMetadata = requestMetadata,
      requestMessage = messageBytes,
      timeout = timeout,
    )

    responseMetadata = response.responseMetadata

    if (response.status != 200) {
      throw IOException("HTTP ${response.status}")
    }

    if (response.grpcStatus != 0) {
      throw GrpcException(GrpcStatus.get(response.grpcStatus), response.grpcMessage, response.grpcStatusDetails)
    }

    if (response.responseMessage == null) {
      throw IOException("expected 1 message but got none")
    }

    return method.responseAdapter.decode(response.responseMessage)
  }

  override fun executeBlocking(request: S): R = throw UnsupportedOperationException("executeBlocking is not supported on Native")

  override fun enqueue(request: S, callback: GrpcCall.Callback<S, R>) {
    check(!executed) { "already executed" }
    executed = true

    GlobalScope.launch {
      try {
        val result = execute(request)
        callback.onSuccess(this@NativeGrpcCall, result)
      } catch (e: Exception) {
        callback.onFailure(this@NativeGrpcCall, IOException(e.message))
      }
    }
  }

  override fun isExecuted(): Boolean = executed

  override fun clone(): GrpcCall<S, R> {
    val result = NativeGrpcCall(grpcClient, method)
    result.requestMetadata += this.requestMetadata
    return result
  }
}
