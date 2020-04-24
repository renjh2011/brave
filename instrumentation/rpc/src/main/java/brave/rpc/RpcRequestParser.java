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
package brave.rpc;

import brave.SpanCustomizer;
import brave.propagation.TraceContext;

/**
 * Use this to control the request data recorded for an {@link TraceContext#sampledLocal() sampled
 * RPC client or server span}.
 *
 * <p>Here's an example that only sets the span name, with no tags:
 * <pre>{@code
 * rpcTracing = rpcTracing.toBuilder()
 *   .clientRequestParser((req, context, span) -> {
 *      String method = req.method();
 *      if (method != null) span.name(method);
 *   }).build();
 * }</pre>
 *
 * @see RpcResponseParser
 */
// @FunctionalInterface: do not add methods as it will break api
public interface RpcRequestParser {
  RpcRequestParser DEFAULT = new Default();

  /**
   * Implement to choose what data from the rpc request are parsed into the span representing it.
   *
   * @see Default
   */
  void parse(RpcRequest request, TraceContext context, SpanCustomizer span);

  /**
   * The default data policy sets the span name to {@code ${rpc.service}/${rpc.method}} or only the
   * method or service. This also tags "rpc.service" and "rpc.method" when present.
   */
  class Default implements RpcRequestParser {
    @Override public void parse(RpcRequest req, TraceContext context, SpanCustomizer span) {
      String service = RpcTags.SERVICE.tag(req, context, span);
      String method = RpcTags.METHOD.tag(req, context, span);
      if (service == null && method == null) return;
      if (service == null) {
        span.name(method);
      } else if (method == null) {
        span.name(service);
      } else {
        span.name(service + "/" + method);
      }
    }
  }
}
