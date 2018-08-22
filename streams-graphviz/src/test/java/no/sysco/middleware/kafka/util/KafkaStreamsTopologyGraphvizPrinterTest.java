package no.sysco.middleware.kafka.util;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.junit.Test;

import java.util.function.Supplier;

import static java.lang.System.out;
import static org.junit.Assert.assertTrue;

public class KafkaStreamsTopologyGraphvizPrinterTest {

  private final Supplier<StreamsBuilder> simpleStreamSupplier =
      () -> {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").to("output-topic");
        return builder;
      };

  @Test
  public void shouldPrintWhenSimpleTopologyIsValid () {
    Topology topology = simpleStreamSupplier.get().build();

    String graphviz = KafkaStreamsTopologyGraphvizPrinter.print(topology);
    out.println(graphviz);
    assertTrue(graphviz.startsWith("digraph kafka_streams_topology {"));
    assertTrue(graphviz.endsWith("}"));
  }

  @Test
  public void shouldPrintAsPlantUmlWhenSimpleTopologyIsValid () {
    Topology topology = simpleStreamSupplier.get().build();

    String graphviz = KafkaStreamsTopologyGraphvizPrinter.printPlantUml(topology);
    out.println(graphviz);
    assertTrue(graphviz.startsWith("@startuml"));
    assertTrue(graphviz.endsWith("@enduml"));
  }
}
