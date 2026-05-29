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

actual class GrpcRequest(
  val url: GrpcHttpUrl,
  val method: String,
  val headers: Map<String, String>,
  val body: GrpcRequestBody?,
)

actual open class GrpcRequestBuilder {
  private var url: GrpcHttpUrl? = null
  private var method: String = "POST"
  private var headers: MutableMap<String, String> = mutableMapOf()
  private var body: GrpcRequestBody? = null

  actual open fun url(url: GrpcHttpUrl): GrpcRequestBuilder = apply {
    this.url = url
  }
  actual open fun addHeader(
    name: String,
    value: String,
  ): GrpcRequestBuilder = apply {
    this.headers[name] = value
  }
  actual open fun method(
    method: String,
    body: GrpcRequestBody?,
  ): GrpcRequestBuilder = apply {
    this.method = method
    this.body = body
  }
  actual open fun build(): GrpcRequest = GrpcRequest(
    url = url ?: throw IllegalArgumentException("url is required"),
    method = method,
    headers = headers,
    body = body,
  )
}
