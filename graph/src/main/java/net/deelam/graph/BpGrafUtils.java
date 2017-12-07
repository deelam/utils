package net.deelam.graph;

import static net.deelam.graph.GrafTxn.tryAndCloseTxn;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;

import lombok.extern.slf4j.Slf4j;

/**
 * Operations on Blueprints graphs
 */
@Slf4j
public final class BpGrafUtils {

  //// Read-only operations

  public static boolean hasSameEndpoints(Edge e, Edge e2) {
    Vertex outV = e2.getVertex(Direction.OUT);
    Vertex inV = e2.getVertex(Direction.IN);
    return hasEndpoints(e2, outV, inV);
  }

  public static boolean hasEndpoints(Edge e, Vertex outV, Vertex inV) {
    Vertex outV1 = e.getVertex(Direction.OUT);
    Vertex inV1 = e.getVertex(Direction.IN);
    boolean equalsEndpoints = true;
    if (!outV1.equals(outV)) {
      log.error("outVertex not equal: " + outV1 + "!=" + outV);
      equalsEndpoints = false;
    }
    if (!inV1.equals(inV)) {
      log.error("inVertex not equal: " + inV1 + "!=" + inV);
      equalsEndpoints = false;
    }
    return equalsEndpoints;
  };

  // returns whether currGraph is nested at some depth within g
  public static boolean isWrappedWithin(Graph g, Graph currGraph) {
    while (g instanceof WrapperGraph) {
      g = ((WrapperGraph<?>) g).getBaseGraph();
      if (g == currGraph)
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Graph> T unwrapToGraphType(Class<T> clazz, Graph g) {
    if (clazz.isInstance(g))
      return (T) g;

    while (g instanceof WrapperGraph) {
      g = ((WrapperGraph<?>) g).getBaseGraph();
      if (clazz.isInstance(g))
        return (T) g;
    }
    return null;
  }

  /// WRITE operations

  public static void clearGraph(TransactionalGraph tgraph) {
    tryAndCloseTxn(tgraph, graph-> {
      for (Edge e : graph.getEdges()) {
        graph.removeEdge(e);
      }

      for (Vertex v : graph.getVertices()) {
        graph.removeVertex(v);
      }
    });
  }

  ///

  public static final Direction[] BOTHDIR = {Direction.OUT, Direction.IN};

  /**
   * Assumes both nodes are in the same graph.
   * Removes original edges and node.
   * @param propMerger to merge node and edge properties
   */
  /*
  public static void mergeNodesAndEdges(Vertex origV, Vertex targetV, boolean excludeNewSelfEdges, IdGraph<?> graph,
      PropertyMerger propMerger) {
    if (origV.equals(targetV)) {
      throw new IllegalArgumentException("origV and targetV are the same nodes (or at least have the same ids)");
    }
    propMerger.mergeProperties(origV, targetV);
    moveEdges(origV, targetV, excludeNewSelfEdges, graph);

    log.debug("Removing node={} that was merged into node={}", origV, targetV);
    graph.removeVertex(origV);
    graph.commit(); // must commit since vertex was removed in case this is called in a getVertices() loop so it will skip removed node
  }
  
  public static void copyNodesAndEdges(Vertex origV, Vertex targetV, String edgePrefix, boolean excludeNewSelfEdges, IdGraph<?> graph,
      PropertyMerger propMerger) {
    if (origV.equals(targetV)) {
      throw new IllegalArgumentException("origV and targetV are the same nodes (or at least have the same ids)");
    }
    propMerger.mergeProperties(origV, targetV);
    copyEdges(origV, targetV, edgePrefix, excludeNewSelfEdges, graph);

    graph.commit(); // must commit since vertex was removed in case this is called in a getVertices() loop so it will skip removed node
  }

  public static void moveEdges(Vertex origV, Vertex targetV, boolean excludeNewSelfEdges, IdGraph<?> graph) {
    for (Direction dir : BOTHDIR)
      for (Edge edge : origV.getEdges(dir)) {
        Vertex neighbor = edge.getVertex(dir.opposite());
        // copy edge in memory before removing from graph
        GraphRecordEdge inMemEdge = new GraphRecordEdgeImpl((String) edge.getId(), edge.getLabel(),
            (String) targetV.getId(), (String) neighbor.getId());
        ElementHelper.copyProperties(edge, inMemEdge);
        graph.removeEdge(edge);

        Edge eCopy;
        if (excludeNewSelfEdges && targetV.equals(neighbor)) { // whether to include the original edge from origV to targetV, which would be a self-edge
          // don't add edge back into graph
        } else {
          if (dir == Direction.OUT) {
            eCopy = graph.addEdge(inMemEdge.getId(), targetV, neighbor, inMemEdge.getLabel());
          } else {
            eCopy = graph.addEdge(inMemEdge.getId(), neighbor, targetV, inMemEdge.getLabel());
          }
          ElementHelper.copyProperties(inMemEdge, eCopy);
        }
      }
  }
  
  public static void copyEdges(Vertex origV, Vertex targetV, String edgePrefix, boolean excludeNewSelfEdges, IdGraph<?> graph) {
    for (Direction dir : BOTHDIR)
      for (Edge edge : origV.getEdges(dir)) {
        Vertex neighbor = edge.getVertex(dir.opposite());
        // copy edge in memory before removing from graph
        GraphRecordEdge inMemEdge = new GraphRecordEdgeImpl((String) edge.getId(), edge.getLabel(),
            (String) targetV.getId(), (String) neighbor.getId());
        ElementHelper.copyProperties(edge, inMemEdge);

        Edge eCopy;
        if (excludeNewSelfEdges && targetV.equals(neighbor)) { // whether to include the original edge from origV to targetV, which would be a self-edge
          // don't add edge back into graph
        } else {
          if (dir == Direction.OUT) {
            eCopy = graph.addEdge(edgePrefix+inMemEdge.getId(), targetV, neighbor, inMemEdge.getLabel());
          } else {
            eCopy = graph.addEdge(edgePrefix+inMemEdge.getId(), neighbor, targetV, inMemEdge.getLabel());
          }
          ElementHelper.copyProperties(inMemEdge, eCopy);
        }
      }
  }
  */
}
