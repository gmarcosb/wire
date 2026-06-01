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
package com.squareup.wire

import com.squareup.wire.internal.KtorGrpcCall
import io.ktor.client.HttpClient

actual abstract class GrpcClient actual constructor() {
  actual abstract fun <S : Any, R : Any> newCall(method: GrpcMethod<S, R>): GrpcCall<S, R>

  actual abstract fun <S : Any, R : Any> newStreamingCall(method: GrpcMethod<S, R>): GrpcStreamingCall<S, R>

  actual abstract fun <S : Any, R : Any> newClientStreamingCall(method: GrpcMethod<S, R>): GrpcClientStreamingCall<S, R>

  actual abstract fun <S : Any, R : Any> newServerStreamingCall(method: GrpcMethod<S, R>): GrpcServerStreamingCall<S, R>
}

class KtorGrpcClient(
  val client: HttpClient,
  val baseUrl: GrpcHttpUrl,
  val minMessageToCompress: Long = 0L,
) : GrpcClient() {

  override fun <S : Any, R : Any> newCall(method: GrpcMethod<S, R>): GrpcCall<S, R> = KtorGrpcCall(this, method)

  override fun <S : Any, R : Any> newStreamingCall(method: GrpcMethod<S, R>): GrpcStreamingCall<S, R> {
    throw UnsupportedOperationException("Bidirectional streaming is not supported by Ktor engines yet.")
  }

  override fun <S : Any, R : Any> newClientStreamingCall(method: GrpcMethod<S, R>): GrpcClientStreamingCall<S, R> {
    throw UnsupportedOperationException("Client streaming is not supported by Ktor engines yet.")
  }

  override fun <S : Any, R : Any> newServerStreamingCall(method: GrpcMethod<S, R>): GrpcServerStreamingCall<S, R> {
    return com.squareup.wire.internal.KtorGrpcServerStreamingCall(this, method)
  }
}
