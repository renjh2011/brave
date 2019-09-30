/*
 * Copyright 2013-2019 The OpenZipkin Authors
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
package brave.rpc;

import brave.Span;

/**
 * Marks an interface for use in {@link RpcClientHandler#handleSend(RpcClientRequest)}. This gives a
 * standard type to consider when parsing an outgoing context.
 *
 * @see RpcClientResponse
 * @since 5.8
 */
public abstract class RpcClientRequest extends RpcRequest {
  @Override public final Span.Kind spanKind() {
    return Span.Kind.CLIENT;
  }
}
