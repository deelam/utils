package net.deelam.graph;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public interface IdGrafFactory {

  String getScheme();

  default void init(GrafUri graphUri) {}

  /**
   * Open existing or create a new graph
   */
  <T extends KeyIndexableGraph> IdGraph<T> open(GrafUri gUri);

  /**
   * Delete graph if it exists
   */
  void delete(GrafUri gUri) throws IOException;

  /**
   * backs up srcGraphUri to dstGraphUri
   */
  void backup(GrafUri srcGraphUri, GrafUri dstGraphUri) throws IOException;

  /**
   * @param gUri
   * @return whether graph exists
   */
  boolean exists(GrafUri gUri);

  default void shutdown(GrafUri gUri) throws IOException {
    gUri.graph().shutdown();
  }

  PropertyMerger createPropertyMerger();

  default String asString(GrafUri graphUri) {
    return graphUri.origUri;
  }

  static Logger log = LoggerFactory.getLogger(IdGrafFactory.class);

  default void createIndices(GrafUri gUri, BpGrafIndexProps.PropertyKeys pks) {
    checkNotNull(pks);
    IdGraph<?> graph=gUri.graph();
    pks.getVertexKeys().forEach((propKey, params) -> {
      if (!graph.getIndexedKeys(Vertex.class).contains(propKey)) {
        log.info("Creating node key index for {} in graph={}", propKey, graph);
        graph.createKeyIndex(propKey, Vertex.class, params);
      }
    });
    pks.getEdgeKeys().forEach((propKey, params) -> {
      if (!graph.getIndexedKeys(Edge.class).contains(propKey)) {
        log.info("Creating edge key index for {} in graph={}", propKey, graph);
        graph.createKeyIndex(propKey, Edge.class, params);
      }
    });
    graph.commit();
  }
}
