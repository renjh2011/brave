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
package brave.grpc;

import brave.grpc.GrpcPropagation.TagsBin;
import brave.internal.HexCodec;
import brave.propagation.B3Propagation;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static brave.grpc.GrpcPropagation.nameToKey;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class GrpcPropagationBenchmarks {
  static final MethodDescriptor<Void, Void> methodDescriptor =
    MethodDescriptor.<Void, Void>newBuilder()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName("helloworld.Greeter/SayHello")
      .setRequestMarshaller(VoidMarshaller.INSTANCE)
      .setResponseMarshaller(VoidMarshaller.INSTANCE)
      .build();

  static final Propagation<String> b3 = B3Propagation.FACTORY.get();
  static final Injector<GrpcClientRequest> b3Injector = b3.injector(GrpcClientRequest.SETTER);
  static final Extractor<GrpcServerRequest> b3Extractor = b3.extractor(GrpcServerRequest.GETTER);

  static final Propagation<String> both = GrpcPropagation.create(B3Propagation.get());
  static final Injector<GrpcClientRequest> bothInjector = both.injector(GrpcClientRequest.SETTER);
  static final Extractor<GrpcServerRequest> bothExtractor =
    both.extractor(GrpcServerRequest.GETTER);

  static final TraceContext context = TraceContext.newBuilder()
    .traceIdHigh(HexCodec.lowerHexToUnsignedLong("67891233abcdef01"))
    .traceId(HexCodec.lowerHexToUnsignedLong("2345678912345678"))
    .spanId(HexCodec.lowerHexToUnsignedLong("463ac35c9f6413ad"))
    .sampled(true)
    .build();
  static final TraceContext contextWithTags;

  static final Map<String, Key<String>>
    b3NameToKey = nameToKey(b3),
    bothNameToKey = nameToKey(both);

  static final GrpcServerRequest
    incomingB3 = new GrpcServerRequest(b3NameToKey, methodDescriptor, new Metadata()),
    incomingBoth = new GrpcServerRequest(bothNameToKey, methodDescriptor, new Metadata()),
    incomingBothNoTags = new GrpcServerRequest(b3NameToKey, methodDescriptor, new Metadata()),
    nothingIncoming = new GrpcServerRequest(emptyMap(), methodDescriptor, new Metadata());

  static final byte[] tagsBytes;

  static {

    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      bytes.write(0); // version
      bytes.write(0); // field number
      bytes.write("method" .length());
      bytes.write("method" .getBytes(UTF_8));
      bytes.write("helloworld.Greeter/SayHello" .length());
      bytes.write("helloworld.Greeter/SayHello" .getBytes(UTF_8));
      tagsBytes = bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    contextWithTags = context.toBuilder().extra(singletonList(new TagsBin(tagsBytes))).build();
    b3Injector.inject(context, new GrpcClientRequest(b3NameToKey, methodDescriptor)
      .metadata(incomingB3.metadata));
    bothInjector.inject(contextWithTags,
      new GrpcClientRequest(bothNameToKey, methodDescriptor)
        .metadata(incomingBoth.metadata));
    bothInjector.inject(context,
      new GrpcClientRequest(bothNameToKey, methodDescriptor)
        .metadata(incomingBothNoTags.metadata));
  }

  @Benchmark public void inject_b3() {
    GrpcClientRequest request =
      new GrpcClientRequest(b3NameToKey, methodDescriptor).metadata(new Metadata());
    b3Injector.inject(context, request);
  }

  @Benchmark public TraceContextOrSamplingFlags extract_b3() {
    return b3Extractor.extract(incomingBoth);
  }

  @Benchmark public TraceContextOrSamplingFlags extract_b3_nothing() {
    return b3Extractor.extract(nothingIncoming);
  }

  @Benchmark public void inject_both() {
    GrpcClientRequest request =
      new GrpcClientRequest(bothNameToKey, methodDescriptor).metadata(new Metadata());
    bothInjector.inject(contextWithTags, request);
  }

  @Benchmark public void inject_both_no_tags() {
    GrpcClientRequest request =
      new GrpcClientRequest(bothNameToKey, methodDescriptor).metadata(new Metadata());
    bothInjector.inject(context, request);
  }

  @Benchmark public TraceContextOrSamplingFlags extract_both() {
    return bothExtractor.extract(incomingBoth);
  }

  @Benchmark public TraceContextOrSamplingFlags extract_both_nothing() {
    return bothExtractor.extract(nothingIncoming);
  }

  @Benchmark public TraceContextOrSamplingFlags extract_both_no_tags() {
    return bothExtractor.extract(incomingBothNoTags);
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .addProfiler("gc")
      .include(".*" + GrpcPropagationBenchmarks.class.getSimpleName())
      .build();

    new Runner(opt).run();
  }

  enum VoidMarshaller implements MethodDescriptor.Marshaller<Void> {
    INSTANCE;

    @Override public InputStream stream(Void value) {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override public Void parse(InputStream stream) {
      return null;
    }
  }
}
