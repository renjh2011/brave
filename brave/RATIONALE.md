Brave Core Library Rationale
==============

Brave's design lends from past experience and similar open source work.
Quite a lot of decisions were driven by portability with Brave 3, and the
dozens of individuals involved in that. Later decisions focus on what we
learned since the previous major. For example, Brave 5 was the last version of
Brave 4, minus deprecation. This provides continuity, yet allows innovation
through minor revisions.

While many ideas are our own, there are notable aspects borrowed or adapted
from others. It is our goal to cite when we learned something through a prior
library, as that allows people to research any rationale that predates our
usage.

Below is always incomplete, and always improvable. We don't document every
thought as it would betray productivity and make this document unreadable.
Rationale here should be limited to impactful designs, and aspects non-obvious,
non-conventional or subtle.

## Public namespace
Brave 4's public namespace is more defensive that the past, using a package
accessor design from [OkHttp](https://github.com/square/okhttp).

## Stateful Span object
Brave 4 allows you to pass around a Span object which can report itself
to Zipkin when finished. This is better than using thread contexts in
some cases, particularly where many async hops are in use. The Span api
is derived from OpenTracing, narrowed to more cleanly match Zipkin's
abstraction. As a result, a bridge from Brave 4 to OpenTracing v0.20.2
is relatively little code. It should be able to implement future
versions of OpenTracing as well.

## ScopedSpan type
Brave 4.19 introduced `ScopedSpan` which derives influence primarily from
[OpenCensus SpanBuilder.startScopedSpan() Api](https://github.com/census-instrumentation/opencensus-java/blob/f852100ad9b77522fabd3c0b29e78202aed3a26e/api/src/main/java/io/opencensus/trace/SpanBuilder.java#L229), a convenience type to
start work and ensure scope is visible downstream. One main difference here is
we assume the scoped span is used in the same thread, so mark it as such. Also,
we reduce the size of the api to help secure users against code drift.

## Error Handling
Brave 4.19 added `ErrorParser`, internally used by `Span.error(Throwable)`. This
design incubated in [Spring Cloud Sleuth](https://github.com/spring-cloud/spring-cloud-sleuth) prior to becoming a primary type here.
Our care and docs around error handling in general is credit to [Nike Wingtips](https://github.com/Nike-Inc/wingtips#warning-about-error-handling-when-using-try-with-resources-to-autoclose-spans).

### Why we don't tag "error=false"
"error=false" is almost always the result of mis-mapping from OpenTracing who
defined a boolean error bit, but not a string message.
https://github.com/opentracing/specification/blob/master/semantic_conventions.yaml#L15

Zipkin by extension Brave's "error" tag is about the error code or message.
There are very few times you will not know a message of some kind. The only
known examples are some RPC frameworks who only have a boolean bit in some
error cases. End users will typically not be implementing such low-level
instrumentation, so should not be steered towards such an edge case, especially
when more meaningful and better messages almost always exist, in worst case the
type name of the error.

Regardless, no one could define a value range for the "error" (message) tag.
There are far too many different frameworks with different people involved. It
is better to steer instrumentors towards getting the best short error message.

Zipkin's tag system allows empty values, and its query api allows tag key only
searches. When considered in this context, the value of the "error" tag is less
important than its existence. This also provides an option for those few edge
cases where there's only a single bit known.. just set the value to empty.

Since OpenTracing caused people to think about error in boolean terms, it is
worth mentioning some glitches this creates in isolated or aggregate traces.

When someone thinks in terms of boolean, they are led to think "error=false" is
something that should be added on success, or by default. However, this is full
of problems. For example, errors can happen late and depending on backend,
overwriting a false with true can cause problems when aggregating error counts.
Next, even when someone thinks something might be success there is no value
gained in asserting this. For example, one-way RPC give no response back. If
you tag "error=false", it could not only be wrong, but also not correctable
later. In short, it is more likely to cause confusion by adding "error=false"
vs not adding a tag at all.

It is particularly important to understand users writing instrumentation will
make mistakes. If we suggest an erroneous practice, it will hurt them in
surprising ways. For example, what was described above resulted in support load
as OpenTracing users colored all spans red by mistake. Knowing pitfalls is
important, but not steering users into pitfalls is more important.

To avoid leading people into problems, we have never described the "error" tag
in boolean terms. It is the the code or message a person instead. For example,
if you know you have an error, but don't know what it is, tag "error" -> "".
This conveys "I don't know the error message". If instead, we said tag "true",
it would tempt people to think "not false". This would be a slippery slope back
into the "error=false" debacle. In any case, if anyone is in a position where
they are inclined to tag "error" -> "", they probably already need help. As
mentioned above, this is almost never the case in practice.

Users who have a success code, and want to tag it should just use a different
tag name than "error" to convey that. For example, gRPC has a status which
includes success, as does HTTP. In other words, the "error" tag does not need
to be overloaded to say the opposite. The better practice is to use an
appropriate alternative tag, such as "http.status_code".

### Error overrides
In some cases, the "error" code added may be misrepresentative. We formerly had
users complaining about junk service diagrams because 404 was classified as an
error. `HTTP GET` returning 404 is sometimes treated like `Map.containsKey` in
certain REST dialects. You can imagine how aggregate service error stats become
with this in mind.

For this reason, all abstractions we make should have a default mapping, yet
allow users to change the primary "error" status if they know better.

## Recorder architecture
Much of Brave 4's architecture is borrowed from Finagle, whose design
implies a separation between the propagated trace context and the data
collected in process. For example, Brave's MutableSpanMap is the same
overall design as Finagle's. The internals of MutableSpanMap were adapted
from [WeakConcurrentMap](https://github.com/raphw/weak-lock-free).

## Propagation Api
OpenTracing's Tracer type has methods to inject or extract a trace
context from a carrier. While naming is similar, Brave optimizes for
direct integration with request types (such as HTTP request) vs routing
through an intermediate (such as a map). Brave also considers propagation
a separate api from the tracer.

### Deprecation of Propagation K parameter

The Propagation `<K>` parameter allowed integration with requests that had
headers, but didn't use String types. This was over-generalized, as in practice
only gRPC instrumentation ever used this key type (`Metadata.Key`).

Removing this `<K>` parameter dramatically simplifies the model as it removes
the need to explain the key factory and the edge case it supported, which can
be accomplished differently.

For example, and equivalent functionality works via internally converting and
caching gRPC propagation keys like so:
```java
/** Creates constant keys for use in propagating trace identifiers or baggage. */
static Map<String, Key<String>> nameToKey(Propagation<String> propagation) {
  Map<String, Key<String>> result = new LinkedHashMap<>();
  for (String keyName : propagation.keys()) {
    result.put(keyName, Key.of(keyName, Metadata.ASCII_STRING_MARSHALLER));
  }
  for (String keyName : BaggagePropagation.baggageKeyNames(propagation)) {
    result.put(keyName, Key.of(keyName, Metadata.ASCII_STRING_MARSHALLER));
  }
  return result;
}
```

In summary, even though we have a generic parameter, only `Propagation<String>`
will be used in practice. We keep the generic parameter to avoid an API breaks
on a frequently used type, even if seeing it is a distraction and having it at
all was a mistake in hindsight.

## Current Tracer Api
The first design work of `Tracing.currentTracer()` started in [Brave 3](https://github.com/openzipkin/brave/pull/210),
which was itself influenced by Finagle's implicit Tracer api. This feature
is noted as edge-case, when other means to get a reference to a trace are
impossible. The only instrumentation that needed this was JDBC.

Returning a possibly null reference from `Tracing.currentTracer()` implies:
* Users never cache the reference returned (noted in javadocs)
* Less code and type constraints on Tracer vs a lazy forwarding delegate
* Less documentation as we don't have to explain what a Noop tracer does

## CurrentTraceContext Api
The first design of `CurrentTraceContext` was borrowed from `ContextUtils`
in Google's [instrumentation-java](https://github.com/google/instrumentation-java) project.
This was possible because of high collaboration between the projects and
an almost identical goal. Slight departures including naming (which prefers
Guice's naming conventions), and that the object scoped is a TraceContext
vs a Span.

Propagating a trace context instead of a span is a right fit for several reasons:
* `TraceContext` can be created without a reference to a tracer
* Common tasks like making child spans and staining log context only need the context
* Brave's recorder is keyed on context, so there's no feature loss in this choice

## Local Sampling flag
The [Census](https://opencensus.io/) project has a concept of a [SampledSpanStore](https://github.com/census-instrumentation/opencensus-java/blob/master/api/src/main/java/io/opencensus/trace/export/SampledSpanStore.java).
Typically, you configure a span name pattern or choose [individual spans](https://github.com/census-instrumentation/opencensus-java/blob/660e8f375bb483a3eb817940b3aa8534f86da314/api/src/main/java/io/opencensus/trace/EndSpanOptions.java#L65)
for local (in-process) storage. This storage is used to power
administrative pages named zPages. For example, [Tracez](https://github.com/census-instrumentation/opencensus-java/blob/660e8f375bb483a3eb817940b3aa8534f86da314/contrib/zpages/src/main/java/io/opencensus/contrib/zpages/TracezZPageHandler.java#L220)
displays spans sampled locally which have errors or crossed a latency
threshold.

The "sample local" vocab in Census was re-used in Brave to describe the
act of keeping data that isn't going to necessarily end up in Zipkin.

The main difference in Brave is implementation. For example, the `sampledLocal`
flag in Brave is a part of the TraceContext and so can be triggered when
headers are parsed. This is needed to provide custom sampling mechanisms.
Also, Brave has a small core library and doesn't package any specific
storage abstraction, admin pages or latency processors. As such, we can't
define specifically what sampled local means as Census' could. All it
means is that `FinishedSpanHandler` will see data for that trace context.

## FinishedSpanHandler Api
Brave had for a long time re-used zipkin's Reporter library, which is
like Census' SpanExporter in so far that they both allow pushing a specific
format elsewhere, usually to a service. Brave's [FinishedSpanHandler](https://github.com/openzipkin/brave/blob/master/brave/src/main/java/brave/handler/FinishedSpanHandler.java)
is a little more like Census' [SpanExporter.Handler](https://github.com/census-instrumentation/opencensus-java/blob/master/api/src/main/java/io/opencensus/trace/export/SpanExporter.java)
in so far as the structure includes the trace context.. something we need
access to in order to do things like advanced sampling.

Where it differs is that the `MutableSpan` in Brave is.. well.. mutable,
and this allows you to cheaply change data. Also, it isn't a struct based
on a proto, so it can hold references to objects like `Exception`. This
allows us to render data into different formats such as [Amazon's stack frames](https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html)
without having to guess what will be needed by parsing up front. As
error parsing is deferred, overhead is less in cases where errors are not
recorded (such as is the case on spans intentionally dropped).

## Rate-limiting sampler
`RateLimitingSampler` was made to allow Amazon X-Ray rules to be
expressed in Brave. We considered their [Reservoir design](https://github.com/aws/aws-xray-sdk-java/blob/2.0.1/aws-xray-recorder-sdk-core/src/main/java/com/amazonaws/xray/strategy/sampling/reservoir/Reservoir.java).
Our implementation differs as it removes a race condition and attempts
to be more fair by distributing accept decisions every decisecond.

## Baggage
The name Baggage was first introduced by Brown University in [Pivot Tracing](https://people.mpi-sws.org/~jcmace/papers/mace2015pivot.pdf)
as maps, sets and tuples. They then spun baggage out as a standalone component,
[BaggageContext](https://people.mpi-sws.org/~jcmace/papers/mace2018universal.pdf)
and considered some of the nuances of making it general purpose. The
implementations proposed in these papers are different to the implementation
here, but conceptually the goal is the same: to propagate "arbitrary stuff"
with a request.

## CorrelationScopeDecorator

### Why hold the initial values even when they match?

The value read at the beginning of a scope is currently held when there's a
chance the field can be updated later:

* `CorrelationField.dirty()`
  * Always revert because someone else could have changed it (ex via `MDC`)
* `CorrelationField.flushOnUpdate()`
  * Revert if only when we changed it (ex via `BaggageField.updateValue()`)

This is because users generally expect data to be "cleaned up" when a scope
completes, even if it was written mid-scope.

https://github.com/spring-cloud/spring-cloud-sleuth/issues/1416

If we delayed reading the value, until update, it could be different, due to
nesting of scopes or out-of-band updates to the correlation context. Hence, we
only have one opportunity to read the value: at scope creation time.

### CorrelationContext

The `CorrelationContext` design is based on pragmatism due to what's available
in underlying log contexts. It mainly avoids operations that accept Map as this
implies overhead to construct and iterate over.

Here is an example source from Log4J 2:
```java
public interface ThreadContextMap {
 void clear();

 boolean containsKey(String var1);

 String get(String var1);

 Map<String, String> getCopy();

 Map<String, String> getImmutableMapOrNull();

 boolean isEmpty();

 void put(String var1, String var2);

 void remove(String var1);
}
```

#### Why not guard on previous value when doing an update?

While the current design is optimized for log contexts (or those similar such
as JFR), you can reasonably think of this like generic contexts such as gRPC
and Armeria:
https://github.com/line/armeria/blob/master/core/src/main/java/com/linecorp/armeria/common/RequestContextStorage.java#L88
https://github.com/grpc/grpc-java/blob/master/context/src/main/java/io/grpc/ThreadLocalContextStorage.java

The above include design facets to help with overlapping scopes, notably
comparing the current value vs the one written prior to reverting a value.

There are two main reasons we aren't doing this is in the current
`CorrelationContext` api. First, this feature was designed for contexts which
don't have these operators. To get a compare-and-set outcome would require
reading back the logging context manually. This has two problems, one is
performance impact and the other is that the value can always be updated
out-of-band. Unlike gRPC context, logging contexts are plain string keys, and
easy to clobber by users or other code. It is therefore hard to tell if
inconsistency is due to things under your control or not (ex bad
instrumentation vs 3rd party code).

The other reason is that this is only used internally where we control the
call sites. These call sites are already try/finally in nature, which addresses
the concern we can control.
