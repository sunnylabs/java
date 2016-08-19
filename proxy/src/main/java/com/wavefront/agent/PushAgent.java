package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;

import com.beust.jcommander.internal.Lists;
import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.formatter.GraphiteFormatter;
import com.wavefront.agent.histogram.accumulator.AccumulationCache;
import com.wavefront.agent.histogram.accumulator.AccumulationTask;
import com.wavefront.agent.histogram.Dispatcher;
import com.wavefront.agent.histogram.DroppingSender;
import com.wavefront.agent.histogram.MapLoader;
import com.wavefront.agent.histogram.QueuingChannelHandler;
import com.wavefront.agent.histogram.TapeDeck;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.GraphiteHostAnnotator;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.ingester.PickleProtocolDecoder;
import com.wavefront.ingester.StreamIngester;
import com.wavefront.ingester.StringLineIngester;
import com.wavefront.ingester.TcpIngester;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import sunnylabs.report.ReportPoint;

/**
 * Push-only Agent.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class PushAgent extends AbstractAgent {

  protected final List<Thread> managedThreads = new ArrayList<>();
  protected final IdentityHashMap<ChannelOption<?>, Object> childChannelOptions = new IdentityHashMap<>();

  public static void main(String[] args) throws IOException {
    // Start the ssh daemon
    new PushAgent().start(args);
  }

  public PushAgent() {
    super(false, true);
  }

  protected PushAgent(boolean reportAsPushAgent) {
    super(false, reportAsPushAgent);
  }

  @Override
  protected void startListeners() {
    if (soLingerTime >= 0) {
      childChannelOptions.put(ChannelOption.SO_LINGER, 0);
    }
    if (pushListenerPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(pushListenerPorts);
      for (String strPort : ports) {
        startGraphiteListener(strPort, null);
      }
    }
    // For each listener port, set up the entire pipeline

    // TODO change this
    startHistogramListener(
        "2880",
        new File("/Users/timschmidt/agent/"),
        Utils.Granularity.MINUTE,
        new TapeDeck<String>(new FileObjectQueue.Converter<String>() {
          @Override
          public String from(byte[] bytes) throws IOException {
            return new String(bytes);
          }

          @Override
          public void toStream(String s, OutputStream outputStream) throws IOException {
            outputStream.write(s.getBytes("UTF-8"));
          }
        }),
        new TapeDeck<ReportPoint>(new FileObjectQueue.Converter<ReportPoint>() {

          @Override
          public ReportPoint from(byte[] bytes) throws IOException {
            SpecificDatumReader<ReportPoint> reader = new SpecificDatumReader<>(ReportPoint.SCHEMA$);
            org.apache.avro.io.Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

            return reader.read(null, decoder);
          }

          @Override
          public void toStream(ReportPoint tDigest, OutputStream outputStream) throws IOException {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            DatumWriter<ReportPoint> writer = new SpecificDatumWriter<>(ReportPoint.SCHEMA$);

            writer.write(tDigest, encoder);
            encoder.flush();
          }
        })

    );

//    if (histogramMinsListenerPorts != null) {
//      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(histogramMinsListenerPorts);
//      for (String strPort : ports) {
////        startHistogramListener(strPort, null);
//      }
//    }

    GraphiteFormatter graphiteFormatter = null;
    if (graphitePorts != null || picklePorts != null) {
      Preconditions.checkNotNull(graphiteFormat, "graphiteFormat must be supplied to enable graphite support");
      Preconditions.checkNotNull(graphiteDelimiters, "graphiteDelimiters must be supplied to enable graphite support");
      graphiteFormatter = new GraphiteFormatter(graphiteFormat, graphiteDelimiters, graphiteFieldsToRemove);
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(graphitePorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          startGraphiteListener(strPort, graphiteFormatter);
          logger.info("listening on port: " + strPort + " for graphite metrics");
        }
      }
    }
    if (opentsdbPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(opentsdbPorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          startOpenTsdbListener(strPort);
          logger.info("listening on port: " + strPort + " for OpenTSDB metrics");
        }
      }
    }
    if (picklePorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(picklePorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          startPickleListener(strPort, graphiteFormatter);
          logger.info("listening on port: " + strPort + " for pickle protocol metrics");
        }
      }
    }
    if (httpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(httpJsonPorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          try {
            int port = Integer.parseInt(strPort);
            // will immediately start the server.
            JettyHttpContainerFactory.createServer(
                new URI("http://localhost:" + strPort + "/"),
                new ResourceConfig(JacksonFeature.class).
                    register(new JsonMetricsEndpoint(port, hostname, prefix,
                        pushValidationLevel, pushBlockedSamples, getFlushTasks(port))), true);
            logger.info("listening on port: " + strPort + " for HTTP JSON metrics");
          } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to bind to: " + strPort + " for HTTP JSON metrics", e);
          }
        }
      }
    }
    if (writeHttpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(writeHttpJsonPorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          try {
            int port = Integer.parseInt(strPort);
            // will immediately start the server.
            JettyHttpContainerFactory.createServer(
                new URI("http://localhost:" + strPort + "/"),
                new ResourceConfig(JacksonFeature.class).
                    register(new WriteHttpJsonMetricsEndpoint(port, hostname, prefix,
                        pushValidationLevel, pushBlockedSamples, getFlushTasks(port))),
                true);
            logger.info("listening on port: " + strPort + " for Write HTTP JSON metrics");
          } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to bind to: " + strPort + " for Write HTTP JSON metrics", e);
          }
        }
      }
    }
  }

  protected void startOpenTsdbListener(String strPort) {
    final int port = Integer.parseInt(strPort);
    final PostPushDataTimedTask[] flushTasks = getFlushTasks(port);
    ChannelInitializer initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        final ChannelHandler handler = new OpenTSDBPortUnificationHandler(
            new OpenTSDBDecoder("unknown", customSourceTags),
            port, prefix, pushValidationLevel, pushBlockedSamples, flushTasks, opentsdbWhitelistRegex,
            opentsdbBlacklistRegex);
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new PlainTextOrHttpFrameDecoder(handler));
      }
    };
    startAsManagedThread(new TcpIngester(initializer, port).withChildChannelOptions(childChannelOptions));
  }

  protected void startPickleListener(String strPort, GraphiteFormatter formatter) {
    int port = Integer.parseInt(strPort);

    // Set up a custom handler
    ChannelHandler handler = new ChannelByteArrayHandler(
        new PickleProtocolDecoder("unknown", customSourceTags, formatter.getMetricMangler(), port),
        port, prefix, pushValidationLevel, pushBlockedSamples,
        getFlushTasks(port), whitelistRegex, blacklistRegex);

    // create a class to use for StreamIngester to get a new FrameDecoder
    // for each request (not shareable since it's storing how many bytes
    // read, etc)
    // the pickle listener for carbon-relay streams data in its own format:
    //   [Length of pickled data to follow in a 4 byte unsigned int]
    //   [pickled data of the given length]
    //   <repeat ...>
    // the LengthFieldBasedFrameDecoder() parses out the length and grabs
    // <length> bytes from the stream and passes that chunk as a byte array
    // to the decoder.
    class FrameDecoderFactoryImpl implements StreamIngester.FrameDecoderFactory {
      @Override
      public ChannelInboundHandler getDecoder() {
        return new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, 1000000, 0, 4, 0, 4, false);
      }
    }

    startAsManagedThread(new StreamIngester(new FrameDecoderFactoryImpl(), handler, port)
        .withChildChannelOptions(childChannelOptions));
  }

  /**
   * Registers a custom point handler on a particular port.
   *
   * @param strPort       The port to listen on.
   * @param decoder       The decoder to use.
   * @param pointHandler  The handler to handle parsed ReportPoints.
   * @param linePredicate Predicate to reject lines. See {@link com.wavefront.common.MetricWhiteBlackList}
   * @param formatter     Transform function for each line.
   */
  protected void startCustomListener(String strPort, Decoder<String> decoder, PointHandler pointHandler,
                                     Predicate<String> linePredicate,
                                     @Nullable Function<String, String> formatter) {
    int port = Integer.parseInt(strPort);
    ChannelHandler channelHandler = new ChannelStringHandler(decoder, pointHandler, linePredicate, formatter);
    startAsManagedThread(new StringLineIngester(channelHandler, port).withChildChannelOptions(childChannelOptions));
  }

  protected void startGraphiteListener(String strPort,
                                       @Nullable Function<String, String> formatter) {
    int port = Integer.parseInt(strPort);
    // Set up a custom graphite handler, with no formatter
    ChannelHandler graphiteHandler = new ChannelStringHandler(new GraphiteDecoder("unknown", customSourceTags),
        port, prefix, pushValidationLevel, pushBlockedSamples, getFlushTasks(port), formatter, whitelistRegex,
        blacklistRegex);

    if (formatter == null) {
      List<Function<Channel, ChannelHandler>> handler = Lists.newArrayList(1);
      handler.add(new Function<Channel, ChannelHandler>() {
        @Override
        public ChannelHandler apply(Channel input) {
          SocketChannel ch = (SocketChannel) input;
          return new GraphiteHostAnnotator(ch.remoteAddress().getHostName(), customSourceTags);
        }
      });
      startAsManagedThread(new StringLineIngester(handler, graphiteHandler, port)
          .withChildChannelOptions(childChannelOptions));
    } else {
      startAsManagedThread(new StringLineIngester(graphiteHandler, port)
          .withChildChannelOptions(childChannelOptions));
    }
  }


  /**
   * Needs to set up a queueing handler and a consumer/lexer for the queue
   */
  protected void startHistogramListener(
      String portAsString,
      File directory,
      Utils.Granularity granularity,
      TapeDeck<String> receiveDeck,
      TapeDeck<ReportPoint> sendDeck) {
    Preconditions.checkNotNull(portAsString);
    Preconditions.checkNotNull(directory);
    Preconditions.checkArgument(directory.isDirectory(), directory.getAbsolutePath() + " must be a directory!");
    Preconditions.checkArgument(directory.canWrite(), directory.getAbsolutePath() + " must be write-able!");
    Preconditions.checkNotNull(granularity);
    Preconditions.checkNotNull(receiveDeck);
    Preconditions.checkNotNull(sendDeck);
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    int port = Integer.parseInt(portAsString);
    File tapeFile = new File(directory, granularity.name() + "_" + portAsString);
    File outTapeFile = new File(directory, "sendTape");
    ObjectQueue<String> receiveTape = receiveDeck.getTape(tapeFile);
    ObjectQueue<ReportPoint> sendTape = sendDeck.getTape(outTapeFile);
    PointHandler invalidPointHandler = new PointHandlerImpl(port, pushValidationLevel, pushBlockedSamples, prefix, getFlushTasks(port));

    // TODO inject
    MapLoader<Utils.HistogramKey, AgentDigest, Utils.HistogramKeyMarshaller, AgentDigest.AgentDigestMarshaller> loader = new MapLoader<>(
        Utils.HistogramKey.class,
        AgentDigest.class,
        10000000L,
        200D,
        1000D,
        Utils.HistogramKeyMarshaller.get(),
        AgentDigest.AgentDigestMarshaller.get());

    File mapFile = new File(directory, "mapfile");
    ConcurrentMap<Utils.HistogramKey, AgentDigest> map = loader.get(mapFile);

    // Local Cache
    AccumulationCache cache = new AccumulationCache(map, 1000L, null);

    scheduler.scheduleWithFixedDelay(cache.getResolveTask(), 10L, 10L, TimeUnit.MILLISECONDS);

    // Set-up scanner
    AccumulationTask scanTask = new AccumulationTask(
        receiveTape,
        cache.getCache().asMap(),
        new GraphiteDecoder("unknown", customSourceTags),
        invalidPointHandler,
        Validation.Level.valueOf(pushValidationLevel),
        30000L,
        granularity);

    scheduler.scheduleWithFixedDelay(scanTask, 100L, 1L, TimeUnit.MICROSECONDS);

    // Set-up dispatcher
    Dispatcher dispatchTask = new Dispatcher(map, sendTape);
    scheduler.scheduleWithFixedDelay(dispatchTask, 100L, 1L, TimeUnit.MICROSECONDS);

    DroppingSender sendTask = new DroppingSender(sendTape);
    scheduler.scheduleWithFixedDelay(sendTask, 100L, 1L, TimeUnit.MICROSECONDS);

    // Set-up producer
    new Thread(
        new StringLineIngester(
            new QueuingChannelHandler<String>(receiveTape),
            port)).start();


//    // Run the same setup as above but pass a special PointHandler to the ChannelStringHandler to accumulate based on tags and metric
//    ChannelHandler handler = new ChannelStringHandler(
//        new GraphiteDecoder("unknown", customSourceTags),
//        null, // TODO This should be the PointHandler
//        new MetricWhiteBlackList(whitelistRegex, blacklistRegex, portAsString),
//        formatter);
//
//    if (formatter == null) {
//      List<Function<Channel, ChannelHandler>> decoders = Lists.newArrayList(1);
//      decoders.add(new Function<Channel, ChannelHandler>() {
//        @Override
//        public ChannelHandler apply(Channel input) {
//          SocketChannel ch = (SocketChannel) input;
//          return new GraphiteHostAnnotator(ch.remoteAddress().getHostName(), customSourceTags);
//        }
//      });
//      new Thread(new StringLineIngester(decoders, handler, port)).start();
//    } else {
//      new Thread(new StringLineIngester(handler, port)).start();
//    }
  }

  /**
   * Push agent configuration during check-in by the collector.
   *
   * @param config The configuration to process.
   */
  @Override
  protected void processConfiguration(AgentConfiguration config) {
    try {
      agentAPI.agentConfigProcessed(agentId);
      Long pointsPerBatch = config.getPointsPerBatch();
      if (config.getCollectorSetsPointsPerBatch() != null &&
          config.getCollectorSetsPointsPerBatch()) {
        if (pointsPerBatch != null) {
          // if the collector is in charge and it provided a setting, use it
          QueuedAgentService.setSplitBatchSize(pointsPerBatch.intValue());
          PostPushDataTimedTask.setPointsPerBatch(pointsPerBatch.intValue());
          if (pushLogLevel.equals("DETAILED")) {
            logger.info("Agent push batch set to (remotely) " + pointsPerBatch);
          }
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        QueuedAgentService.setSplitBatchSize(pushFlushMaxPoints);
        PostPushDataTimedTask.setPointsPerBatch(pushFlushMaxPoints);
        if (pushLogLevel.equals("DETAILED")) {
          logger.info("Agent push batch set to (locally) " + pushFlushMaxPoints);
        }
      }

      if (config.getCollectorSetsRetryBackoff() != null &&
          config.getCollectorSetsRetryBackoff()) {
        if (config.getRetryBackoffBaseSeconds() != null) {
          // if the collector is in charge and it provided a setting, use it
          QueuedAgentService.setRetryBackoffBaseSeconds(config.getRetryBackoffBaseSeconds());
          if (pushLogLevel.equals("DETAILED")) {
            logger.info("Agent backoff base set to (remotely) " +
                config.getRetryBackoffBaseSeconds());
          }
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        QueuedAgentService.setRetryBackoffBaseSeconds(retryBackoffBaseSeconds);
        if (pushLogLevel.equals("DETAILED")) {
          logger.info("Agent backoff base set to (locally) " + retryBackoffBaseSeconds);
        }
      }
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die.
    }
  }

  protected void startAsManagedThread(Runnable target) {
    Thread thread = new Thread(target);
    managedThreads.add(thread);
    thread.start();
  }

  @Override
  public void stopListeners() {
    for (Thread thread : managedThreads) {
      thread.interrupt();
      try {
        thread.join(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }
}
