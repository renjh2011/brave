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

import brave.Span;
import brave.rpc.RpcClientRequest;
import java.util.Map;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;

final class DubboClientRequest extends RpcClientRequest implements DubboRequest {
  final Invoker<?> invoker;
  final Invocation invocation;
  final Map<String, String> attachments;

  DubboClientRequest(Invoker<?> invoker, Invocation invocation) {
    if (invoker == null) throw new NullPointerException("invoker == null");
    if (invocation == null) throw new NullPointerException("invocation == null");
    this.invoker = invoker;
    this.invocation = invocation;
    // When A service invoke B service, then B service then invoke C service, the parentId of the
    // C service span is A when read from invocation.getAttachments(). This is because
    // AbstractInvoker adds attachments via RpcContext.getContext(), not the invocation.
    // See org.apache.dubbo.rpc.protocol.AbstractInvoker(line 141) from v2.7.3
    this.attachments = RpcContext.getContext().getAttachments();
  }

  @Override public Invoker<?> invoker() {
    return invoker;
  }

  @Override public Invocation invocation() {
    return invocation;
  }

  /** Returns the {@link Invocation}. */
  @Override public Invocation unwrap() {
    return invocation;
  }

  /**
   * Returns the method name of the invocation or the first string arg of an "$invoke" or
   * "$invokeAsync" method.
   */
  @Override public String method() {
    return DubboParser.method(invocation);
  }

  /**
   * Returns the {@link URL#getServiceInterface() service interface} of the invocation.
   */
  @Override public String service() {
    return DubboParser.service(invocation);
  }

  @Override public boolean parseRemoteIpAndPort(Span span) {
    return DubboParser.parseRemoteIpAndPort(span);
  }

  @Override protected void propagationField(String keyName, String value) {
    attachments.put(keyName, value);
  }
}
