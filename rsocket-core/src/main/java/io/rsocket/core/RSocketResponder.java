/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder extends RequesterResponderSupport implements RSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketResponder.class);

  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  private final DuplexConnection connection;
  private final RSocket requestHandler;

  private volatile Throwable terminationError;
  private static final AtomicReferenceFieldUpdater<RSocketResponder, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketResponder.class, Throwable.class, "terminationError");

  RSocketResponder(
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize) {
    super(mtu, maxFrameLength, maxInboundPayloadSize, payloadDecoder, connection.alloc(), null);

    this.connection = connection;
    this.requestHandler = requestHandler;

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    UnboundedProcessor<ByteBuf> sendProcessor = super.getSendProcessor();

    connection.send(sendProcessor).subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleFrame, e -> {});

    connection
        .onClose()
        .subscribe(null, this::tryTerminateOnConnectionError, this::tryTerminateOnConnectionClose);
  }

  private void handleSendProcessorError(Throwable t) {
    for (FrameHandler frameHandler : activeStreams.values()) {
      frameHandler.handleError(t);
    }
  }

  private void tryTerminateOnConnectionError(Throwable e) {
    tryTerminate(() -> e);
  }

  private void tryTerminateOnConnectionClose() {
    tryTerminate(() -> CLOSED_CHANNEL_EXCEPTION);
  }

  private void tryTerminate(Supplier<Throwable> errorSupplier) {
    if (terminationError == null) {
      Throwable e = errorSupplier.get();
      if (TERMINATION_ERROR.compareAndSet(this, null, e)) {
        doOnDispose(e);
      }
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      return requestHandler.fireAndForget(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      return requestHandler.requestResponse(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      return requestHandler.requestStream(payload);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      return requestHandler.requestChannel(payloads);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return requestHandler.metadataPush(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public void dispose() {
    tryTerminate(() -> new CancellationException("Disposed"));
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  void doOnDispose(Throwable e) {
    cleanUpSendingSubscriptions();

    connection.dispose();
    requestHandler.dispose();
    super.getSendProcessor().dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    activeStreams.values().forEach(FrameHandler::handleCancel);
    activeStreams.clear();
  }

  void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderCodec.streamId(frame);
      FrameHandler receiver;
      FrameType frameType = FrameHeaderCodec.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, frame);
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, frame);
          break;
        case REQUEST_STREAM:
          long streamInitialRequestN = RequestStreamFrameCodec.initialRequestN(frame);
          handleStream(streamId, frame, streamInitialRequestN);
          break;
        case REQUEST_CHANNEL:
          long channelInitialRequestN = RequestChannelFrameCodec.initialRequestN(frame);
          handleChannel(streamId, frame, channelInitialRequestN, false);
          break;
        case REQUEST_CHANNEL_COMPLETE:
          long completeChannelInitialRequestN = RequestChannelFrameCodec.initialRequestN(frame);
          handleChannel(streamId, frame, completeChannelInitialRequestN, true);
          break;
        case METADATA_PUSH:
          handleMetadataPush(metadataPush(super.getPayloadDecoder().apply(frame)));
          break;
        case CANCEL:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleCancel();
          }
          break;
        case REQUEST_N:
          receiver = super.get(streamId);
          if (receiver != null) {
            long n = RequestNFrameCodec.requestN(frame);
            receiver.handleRequestN(n);
          }
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case NEXT:
          receiver = super.get(streamId);
          if (receiver != null) {
            boolean hasFollows = FrameHeaderCodec.hasFollows(frame);
            receiver.handleNext(frame, hasFollows, false);
          }
          break;
        case COMPLETE:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleComplete();
          }
          break;
        case ERROR:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleError(io.rsocket.exceptions.Exceptions.from(streamId, frame));
          }
          break;
        case NEXT_COMPLETE:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleNext(frame, false, true);
          }
          break;
        case SETUP:
          super.getSendProcessor()
              .onNext(
                  ErrorFrameCodec.encode(
                      super.getAllocator(),
                      streamId,
                      new IllegalStateException("Setup frame received post setup.")));
          break;
        case LEASE:
        default:
          super.getSendProcessor()
              .onNext(
                  ErrorFrameCodec.encode(
                      super.getAllocator(),
                      streamId,
                      new IllegalStateException(
                          "ServerRSocket: Unexpected frame type: " + frameType)));
          break;
      }
    } catch (Throwable t) {
      LOGGER.error("Unexpected error during frame handling", t);
      super.getSendProcessor()
          .onNext(
              ErrorFrameCodec.encode(
                  super.getAllocator(),
                  0,
                  new ConnectionErrorException("Unexpected error during frame handling", t)));
      this.tryTerminateOnConnectionError(t);
    }
  }

  void handleFireAndForget(int streamId, ByteBuf frame) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      FireAndForgetResponderSubscriber subscriber =
          new FireAndForgetResponderSubscriber(streamId, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      fireAndForget(super.getPayloadDecoder().apply(frame))
          .subscribe(FireAndForgetResponderSubscriber.INSTANCE);
    }
  }

  void handleRequestResponse(int streamId, ByteBuf frame) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      RequestResponseResponderSubscriber subscriber =
          new RequestResponseResponderSubscriber(streamId, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      RequestResponseResponderSubscriber subscriber =
          new RequestResponseResponderSubscriber(streamId, this);

      if (this.add(streamId, subscriber)) {
        this.requestResponse(super.getPayloadDecoder().apply(frame)).subscribe(subscriber);
      }
    }
  }

  void handleStream(int streamId, ByteBuf frame, long initialRequestN) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      RequestStreamResponderSubscriber subscriber =
          new RequestStreamResponderSubscriber(streamId, initialRequestN, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      RequestStreamResponderSubscriber subscriber =
          new RequestStreamResponderSubscriber(streamId, initialRequestN, this);

      if (this.add(streamId, subscriber)) {
        this.requestStream(super.getPayloadDecoder().apply(frame)).subscribe(subscriber);
      }
    }
  }

  void handleChannel(int streamId, ByteBuf frame, long initialRequestN, boolean complete) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      RequestChannelResponderSubscriber subscriber =
          new RequestChannelResponderSubscriber(streamId, initialRequestN, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      final Payload firstPayload = super.getPayloadDecoder().apply(frame);
      RequestChannelResponderSubscriber subscriber =
          new RequestChannelResponderSubscriber(streamId, initialRequestN, firstPayload, this);

      if (this.add(streamId, subscriber)) {
        this.requestChannel(subscriber).subscribe(subscriber);
        if (complete) {
          subscriber.handleComplete();
        }
      }
    }
  }

  final void handleMetadataPush(Mono<Void> result) {
    result.subscribe(MetadataPushResponderSubscriber.INSTANCE);
  }

  final boolean add(int streamId, FrameHandler frameHandler) {
    FrameHandler existingHandler;
    synchronized (this) {
      existingHandler = super.activeStreams.putIfAbsent(streamId, frameHandler);
    }

    if (existingHandler != null) {
      frameHandler.handleCancel();
      return false;
    }

    return true;
  }
}
