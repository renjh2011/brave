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
import brave.Span.Kind;
import brave.SpanCustomizer;
import brave.Tag;
import brave.Tags;
import brave.Tracer;
import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import brave.rpc.RpcClientHandler;
import brave.rpc.RpcResponse;
import brave.rpc.RpcResponseParser;
import brave.rpc.RpcServerHandler;
import brave.rpc.RpcTracing;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.config.spring.extension.SpringExtensionFactory;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;

import static brave.internal.Throwables.propagateIfFatal;

@Activate(group = {CommonConstants.PROVIDER, CommonConstants.CONSUMER}, value = "tracing")
// http://dubbo.apache.org/en-us/docs/dev/impls/filter.html
// public constructor permitted to allow dubbo to instantiate this
public final class TracingFilter implements Filter {
  static final Tag<RpcResponse> DUBBO_ERROR_CODE = new Tag<RpcResponse>("dubbo.error_code") {
    @Override protected String parseValue(RpcResponse input, TraceContext context) {
      return input.errorCode();
    }
  };
  static final RpcResponseParser LEGACY_RESPONSE_PARSER = new RpcResponseParser() {
    @Override public void parse(RpcResponse response, TraceContext context, SpanCustomizer span) {
      String errorCode = DUBBO_ERROR_CODE.tag(response, span);
      if (errorCode != null && response.error() == null) {
        span.tag(Tags.ERROR.key(), errorCode);
      }
    }
  };

  CurrentTraceContext currentTraceContext;
  Tracer tracer;
  RpcClientHandler clientHandler;
  RpcServerHandler serverHandler;
  volatile boolean isInit = false;

  /**
   * {@link ExtensionLoader} supplies the tracing implementation which must be named "tracing". For
   * example, if using the {@link SpringExtensionFactory}, only a bean named "tracing" will be
   * injected.
   */
  public void setTracing(Tracing tracing) {
    if (tracing == null) throw new NullPointerException("rpcTracing == null");
    if (isInit) return; // don't override an existing Rpc Tracing
    setRpcTracing(RpcTracing.newBuilder(tracing)
      .clientResponseParser(LEGACY_RESPONSE_PARSER)
      .serverResponseParser(LEGACY_RESPONSE_PARSER)
      .build());
  }

  /**
   * {@link ExtensionLoader} supplies the tracing implementation which must be named "rpcTracing".
   * For example, if using the {@link SpringExtensionFactory}, only a bean named "rpcTracing" will
   * be injected.
   */
  public void setRpcTracing(RpcTracing rpcTracing) {
    if (rpcTracing == null) throw new NullPointerException("rpcTracing == null");
    // we don't guard on init because we intentionally want to overwrite any call to setTracing
    currentTraceContext = rpcTracing.tracing().currentTraceContext();
    tracer = rpcTracing.tracing().tracer();
    clientHandler = RpcClientHandler.create(rpcTracing);
    serverHandler = RpcServerHandler.create(rpcTracing);
    isInit = true;
  }

  @Override
  public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
    if (!isInit) return invoker.invoke(invocation);

    RpcContext rpcContext = RpcContext.getContext();
    Kind kind = rpcContext.isProviderSide() ? Kind.SERVER : Kind.CLIENT;
    final Span span;
    DubboClientRequest clientRequest = null;
    DubboServerRequest serverRequest = null;
    if (kind.equals(Kind.CLIENT)) {
      clientRequest = new DubboClientRequest(invoker, invocation);
      span = clientHandler.handleSend(clientRequest);
    } else {
      serverRequest = new DubboServerRequest(invoker, invocation);
      span = serverHandler.handleReceive(serverRequest);
    }

    boolean deferFinish = false;
    Scope scope = currentTraceContext.newScope(span.context());
    Result result = null;
    Throwable error = null;
    try {
      result = invoker.invoke(invocation);
      error = result.getException();
      Future<Object> future = rpcContext.getFuture(); // the case on async client invocation
      if (future instanceof CompletableFuture) {
        deferFinish = true;
        if (clientRequest != null) {
          ((CompletableFuture<?>) future).whenComplete(
            new FinishClientRequest(clientHandler, clientRequest, result, span));
        } else {
          ((CompletableFuture<?>) future).whenComplete(
            new FinishServerRequest(serverHandler, serverRequest, result, span));
        }
      }
      return result;
    } catch (Throwable e) {
      propagateIfFatal(e);
      error = e;
      throw e;
    } finally {
      if (!deferFinish) {
        if (clientRequest != null) {
          clientHandler.handleReceive(new DubboClientResponse(clientRequest, result, error), span);
        } else {
          serverHandler.handleSend(new DubboServerResponse(serverRequest, result, error), span);
        }
      }
      scope.close();
    }
  }

  static final class FinishClientRequest implements BiConsumer<Object, Throwable> {
    final RpcClientHandler clientHandler;
    final DubboClientRequest clientRequest;
    final Result result;
    final Span span;

    FinishClientRequest(RpcClientHandler clientHandler, DubboClientRequest clientRequest,
      Result result, Span span) {
      this.clientHandler = clientHandler;
      this.clientRequest = clientRequest;
      this.result = result;
      this.span = span;
    }

    @Override public void accept(Object o, @Nullable Throwable error) {
      clientHandler.handleReceive(new DubboClientResponse(clientRequest, result, error), span);
    }
  }

  static final class FinishServerRequest implements BiConsumer<Object, Throwable> {
    final RpcServerHandler serverHandler;
    final DubboServerRequest serverRequest;
    final Result result;
    final Span span;

    FinishServerRequest(RpcServerHandler serverHandler, DubboServerRequest serverRequest,
      Result result, Span span) {
      this.serverHandler = serverHandler;
      this.serverRequest = serverRequest;
      this.result = result;
      this.span = span;
    }

    @Override public void accept(Object o, @Nullable Throwable error) {
      serverHandler.handleSend(new DubboServerResponse(serverRequest, result, error), span);
    }
  }
}
