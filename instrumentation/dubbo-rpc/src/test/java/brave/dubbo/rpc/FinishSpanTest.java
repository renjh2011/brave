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
package brave.dubbo.rpc;

import brave.Span;
import com.alibaba.dubbo.rpc.Result;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Span.Kind;

import static org.mockito.Mockito.mock;

public class FinishSpanTest extends ITTracingFilter {
  TracingFilter filter;

  @Before public void setup() {
    filter = init();
  }

  @Test public void finish_null_result_and_error_DubboClientRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).start();

    FinishSpan.finish(filter, mock(DubboClientRequest.class), null, null, span);

    reporter.takeRemoteSpan(Kind.CLIENT);
  }

  @Test public void finish_null_result_and_error_DubboServerRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.SERVER).start();

    FinishSpan.finish(filter, mock(DubboServerRequest.class), null, null, span);

    reporter.takeRemoteSpan(Kind.SERVER);
  }

  @Test public void finish_result_but_null_error_DubboClientRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).start();

    FinishSpan.finish(filter, mock(DubboClientRequest.class), mock(Result.class), null, span);

    reporter.takeRemoteSpan(Kind.CLIENT);
  }

  @Test public void finish_result_but_null_error_DubboServerRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.SERVER).start();

    FinishSpan.finish(filter, mock(DubboServerRequest.class), mock(Result.class), null, span);

    reporter.takeRemoteSpan(Kind.SERVER);
  }

  @Test public void finish_error_but_null_result_DubboClientRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).start();

    Throwable error = new RuntimeException("melted");
    FinishSpan.finish(filter, mock(DubboClientRequest.class), null, error, span);

    reporter.takeRemoteSpanWithError(Kind.CLIENT, error.getMessage());
  }

  @Test public void finish_error_but_null_result_DubboServerRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.SERVER).start();

    Throwable error = new RuntimeException("melted");
    FinishSpan.finish(filter, mock(DubboServerRequest.class), null, error, span);

    reporter.takeRemoteSpanWithError(Kind.SERVER, error.getMessage());
  }

  @Test public void create_null_result_value_and_error_DubboClientRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).start();

    FinishSpan.create(filter, mock(DubboClientRequest.class), mock(Result.class), span)
      .finish(null, null);

    reporter.takeRemoteSpan(Kind.CLIENT);
  }

  @Test public void create_null_result_value_and_error_DubboServerRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.SERVER).start();

    FinishSpan.create(filter, mock(DubboServerRequest.class), mock(Result.class), span)
      .finish(null, null);

    reporter.takeRemoteSpan(Kind.SERVER);
  }

  @Test public void create_result_value_but_null_error_DubboClientRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).start();

    FinishSpan.create(filter, mock(DubboClientRequest.class), mock(Result.class), span)
      .finish(new Object(), null);

    reporter.takeRemoteSpan(Kind.CLIENT);
  }

  @Test public void create_result_value_but_null_error_DubboServerRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.SERVER).start();

    FinishSpan.create(filter, mock(DubboServerRequest.class), mock(Result.class), span)
      .finish(new Object(), null);

    reporter.takeRemoteSpan(Kind.SERVER);
  }

  @Test public void create_error_but_null_result_value_DubboClientRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).start();

    Throwable error = new RuntimeException("melted");
    FinishSpan.create(filter, mock(DubboClientRequest.class), mock(Result.class), span)
      .finish(null, error);

    reporter.takeRemoteSpanWithError(Kind.CLIENT, error.getMessage());
  }

  @Test public void create_error_but_null_result_value_DubboServerRequest() {
    Span span = tracing.tracer().nextSpan().kind(Span.Kind.SERVER).start();

    Throwable error = new RuntimeException("melted");
    FinishSpan.create(filter, mock(DubboServerRequest.class), mock(Result.class), span)
      .finish(null, error);

    reporter.takeRemoteSpanWithError(Kind.SERVER, error.getMessage());
  }
}