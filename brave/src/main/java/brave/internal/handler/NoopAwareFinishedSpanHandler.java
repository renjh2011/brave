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
package brave.internal.handler;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static brave.internal.Throwables.propagateIfFatal;

/**
 * When {@code noop}, this drops input spans by returning false. Otherwise, it logs exceptions
 * instead of raising an error, as the supplied handler could have bugs.
 */
public final class NoopAwareFinishedSpanHandler extends FinishedSpanHandler {
  public static FinishedSpanHandler create(Set<FinishedSpanHandler> handlers, AtomicBoolean noop) {
    FinishedSpanHandler[] handlersArray = handlers.toArray(new FinishedSpanHandler[0]);
    if (handlersArray.length == 0) return FinishedSpanHandler.NOOP;

    boolean alwaysSampleLocal = false, supportsOrphans = false;
    for (FinishedSpanHandler handler : handlers) {
      if (handler.alwaysSampleLocal()) alwaysSampleLocal = true;
      if (handler.supportsOrphans()) supportsOrphans = true;
    }

    FinishedSpanHandler handler;
    if (handlersArray.length == 1) {
      handler = handlersArray[0];
    } else {
      handler = new CompositeFinishedSpanHandler(handlersArray);
    }
    return new NoopAwareFinishedSpanHandler(handler, noop, alwaysSampleLocal, supportsOrphans);
  }

  final FinishedSpanHandler delegate;
  final AtomicBoolean noop;
  boolean alwaysSampleLocal, supportsOrphans;

  NoopAwareFinishedSpanHandler(FinishedSpanHandler delegate, AtomicBoolean noop,
    boolean alwaysSampleLocal, boolean supportsOrphans) {
    this.delegate = delegate;
    this.noop = noop;
    this.alwaysSampleLocal = alwaysSampleLocal;
    this.supportsOrphans = supportsOrphans;
  }

  @Override public final boolean handle(TraceContext context, MutableSpan span) {
    if (noop.get()) return false;
    try {
      return delegate.handle(context, span);
    } catch (Throwable t) {
      propagateIfFatal(t);
      Platform.get().log("error handling {0}", context, t);
      return false;
    }
  }

  @Override public final boolean alwaysSampleLocal() {
    return alwaysSampleLocal;
  }

  @Override public final boolean supportsOrphans() {
    return supportsOrphans;
  }

  @Override public String toString() {
    return delegate.toString();
  }

  static final class CompositeFinishedSpanHandler extends FinishedSpanHandler {
    final FinishedSpanHandler[] handlers; // Array ensures no iterators are created at runtime

    CompositeFinishedSpanHandler(FinishedSpanHandler[] handlers) {
      this.handlers = handlers;
    }

    @Override public boolean handle(TraceContext context, MutableSpan span) {
      for (FinishedSpanHandler handler : handlers) {
        if (!handler.handle(context, span)) return false;
      }
      return true;
    }

    @Override public String toString() {
      return Arrays.toString(handlers);
    }
  }
}
