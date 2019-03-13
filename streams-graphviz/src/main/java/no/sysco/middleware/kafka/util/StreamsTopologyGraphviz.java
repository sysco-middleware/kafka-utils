package no.sysco.middleware.kafka.util;

import io.github.livingdocumentation.dotdiagram.DotGraph;
import java.util.stream.Stream;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

/**
 * Utility for printing Kafka Streams Topologies in Graphviz format.
 */
public class StreamsTopologyGraphviz {

  /**
   * Creates a Graphviz from Kafka Streams {@link Topology}
   *
   * @return Graphviz text
   */
  public static String print(Topology topology) {
    final TopologyDescription topologyDescription = topology.describe();
    final DotGraph dotGraph = new DotGraph("kafka-streams topology");
    final DotGraph.Digraph digraph = dotGraph.getDigraph();

    printTopics(topologyDescription, digraph);

    printStores(topologyDescription, digraph);

    printGlobalStores(topologyDescription, digraph);

    printSubtopologies(topologyDescription, digraph);

    return dotGraph.render();
  }

  private static void printSubtopologies(TopologyDescription topologyDescription,
      DotGraph.Digraph digraph) {
    topologyDescription
        .subtopologies()
        .forEach(subtopology -> printSubtopology(subtopology, digraph));
  }

  private static void printGlobalStores(TopologyDescription topologyDescription,
      DotGraph.Digraph digraph) {
    topologyDescription.globalStores()
        .forEach(
            globalStore ->
            {
              TopologyDescription.Processor processor = globalStore.processor();

              String processorId = String.format("node-%s", processor.name());
              DotGraph.AbstractNode processorNode = digraph.addNode(processorId)
                  .setLabel(String.format("Processor: %s", processor.name()));

              TopologyDescription.Source predecessor = globalStore.source();
              String predecessorId = String.format("node-%s", predecessor.name());
              processorNode.addAssociation(predecessorId, processorId);

              digraph.addNode(predecessorId)
                  .setLabel(String.format("Source: %s", predecessor.name()));

              final String topics = predecessor.topics();

              Stream.of(topics.substring(1, topics.length() - 1).split(","))
                  .map(String::trim)
                  .forEach(topic -> {
                    String topicId = String.format("topic-%s", topic);
                    digraph.addNode(topicId)
                        .setLabel(String.format("Topic: %s", topic))
                        .addAssociation(topicId, predecessorId);
                  });

              String id = String.format("global-store-%s", globalStore.id());
              DotGraph.AbstractNode abstractNode = digraph
                  .addNode(id)
                  .setLabel(String.format("Global Store: %s", globalStore.id()))
                  .setOptions("shape=box3d");
              abstractNode.addAssociation(processorId, id);
            });
    ;
  }

  private static void printStores(TopologyDescription topologyDescription,
      DotGraph.Digraph digraph) {
    topologyDescription.subtopologies().stream()
        .flatMap(subtopology -> subtopology.nodes().stream())
        .filter(node -> node instanceof TopologyDescription.Processor)
        .flatMap(node -> ((TopologyDescription.Processor) node).stores().stream())
        .distinct()
        .forEach(topic ->
            digraph
                .addNode(String.format("store-%s", topic))
                .setLabel(String.format("Store: %s", topic))
                .setOptions("shape=box3d"));
  }

  private static void printTopics(TopologyDescription topologyDescription,
      DotGraph.Digraph digraph) {
    final Stream<String> sourceTopics =
        topologyDescription.subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .filter(node -> node instanceof TopologyDescription.Source)
            .flatMap(node -> {
              final String topics = ((TopologyDescription.Source) node).topics();
              return Stream.of(topics.substring(1, topics.length() - 1).split(","));
            })
            .map(String::trim);

    final Stream<String> sinkTopics =
        topologyDescription.subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .filter(node -> node instanceof TopologyDescription.Sink)
            .map(node -> ((TopologyDescription.Sink) node).topic());

    Stream.concat(sourceTopics, sinkTopics)
        .distinct()
        .forEach(topic ->
            digraph
                .addNode(String.format("topic-%s", topic))
                .setOptions("shape=cds")
                .setLabel(String.format("Topic: %s", topic)));
  }

  private static void printSubtopology(TopologyDescription.Subtopology subtopology,
      DotGraph.Digraph digraph) {
    DotGraph.Cluster cluster =
        (DotGraph.Cluster) digraph.addCluster(String.format("topology_%s", subtopology.id()))
            .setLabel(String.format("Sub-Topology: %s", subtopology.id()));

    for (TopologyDescription.Node node : subtopology.nodes()) {

      if (node instanceof TopologyDescription.Source) {
        String processorId = String.format("node-%s", node.name());
        cluster.addNode(processorId)
            .setLabel(String.format("Source: %s", node.name()));

        final String topics = ((TopologyDescription.Source) node).topics();

        Stream.of(topics.substring(1, topics.length() - 1).split(","))
            .map(String::trim)
            .forEach(topic -> {
              String topicId = String.format("topic-%s", topic);
              cluster.addNode(topicId)
                  .setLabel(String.format("Topic: %s", topic))
                  .addAssociation(topicId, processorId);
            });
      } else if (node instanceof TopologyDescription.Processor) {
        String processorId = String.format("node-%s", node.name());
        DotGraph.AbstractNode processorNode = cluster.addNode(processorId)
            .setLabel(String.format("Processor: %s", node.name()));

        for (TopologyDescription.Node predecessor : node.predecessors()) {
          String predecessorId = String.format("node-%s", predecessor.name());
          processorNode.addAssociation(predecessorId, processorId);
        }

        for (String store : ((TopologyDescription.Processor) node).stores()) {
          String storeId = String.format("store-%s", store);
          processorNode.addAssociation(processorId, storeId);
        }
      } else if (node instanceof TopologyDescription.Sink) {
        String processorId = String.format("node-%s", node.name());
        DotGraph.AbstractNode sinkNode =
            cluster.addNode(processorId).setLabel(String.format("Sink: %s", node.name()));

        for (TopologyDescription.Node predecessor : node.predecessors()) {
          String predecessorId = String.format("node-%s", predecessor.name());
          sinkNode.addAssociation(predecessorId, processorId);
        }

        String topic = ((TopologyDescription.Sink) node).topic();
        String topicId = String.format("topic-%s", topic);
        sinkNode.addAssociation(processorId, topicId);
      }
    }
  }
}