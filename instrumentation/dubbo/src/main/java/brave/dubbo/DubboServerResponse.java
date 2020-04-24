/*
 * Copyright 2013-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.dubbo;

import brave.internal.Nullable;
import brave.rpc.RpcServerResponse;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

/** @since 5.12 */
public final class DubboServerResponse extends RpcServerResponse {
  final DubboServerRequest request;
  @Nullable final Result result;
  @Nullable final Throwable error;

  DubboServerResponse(DubboServerRequest request, Result result, @Nullable Throwable error) {
    if (request == null) throw new NullPointerException("request == null");
    this.request = request;
    this.result = result;
    this.error = error;
  }

  /** Returns the {@link Result}. */
  @Override public Result unwrap() {
    return result;
  }

  /** {@inheritDoc} */
  @Override public DubboServerRequest request() {
    return request;
  }

  /** {@inheritDoc} */
  @Override public Throwable error() {
    return error;
  }

  /** Returns the string form of the {@link RpcException#getCode()} */
  @Override public String errorCode() {
    return DubboParser.errorCode(error);
  }
}
