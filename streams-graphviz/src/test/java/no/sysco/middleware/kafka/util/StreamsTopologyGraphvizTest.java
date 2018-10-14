package no.sysco.middleware.kafka.util;

import java.util.function.Supplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.junit.Test;

import static java.lang.System.out;
import static org.junit.Assert.assertTrue;

public class StreamsTopologyGraphvizTest {

  private final Supplier<StreamsBuilder> simpleStreamSupplier =
      () -> {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
        return builder;
      };

  @Test
  public void shouldPrintWhenSimpleTopologyIsValid () {
    Topology topology = simpleStreamSupplier.get().build();

    String graphviz = StreamsTopologyGraphviz.print(topology);
    out.println(graphviz);
    assertTrue(graphviz.contains("digraph G {"));
  }
}
