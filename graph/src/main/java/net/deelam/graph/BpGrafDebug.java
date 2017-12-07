package net.deelam.graph;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Operations on Blueprints graphs
 */
public final class BpGrafDebug {

  public static long getNodeCount(Graph graph) {
    return Iterables.size(graph.getVertices());
  }

  public static long getEdgeCount(Graph graph) {
    return Iterables.size(graph.getEdges());
  }

  public static String count(Graph graph) {
    StringBuilder sb = new StringBuilder(graph.toString());
    sb.append(": ");
    sb.append(getNodeCount(graph)).append(" nodes, ");
    sb.append(getEdgeCount(graph)).append(" edges");
    return (sb.toString());
  }

  public static String toString(Graph graph) {
    return toString(graph, -1);
  }

  public static String toString(Graph graph, int numSamples) {
    return toString(graph, numSamples, (String[]) null);
  }

  public static String toString(Graph graph, int numSamples, String... propsToPrint) {
    StringBuilder sb = new StringBuilder(graph.toString());
    sb.append("\n Nodes:\n");
    int nodeCount = 0;
    for (Vertex n : graph.getVertices()) {
      ++nodeCount;
      // Note that IdGraph.ID is removed from the IdElement.propertyKeys() list
      if (numSamples < 0 || nodeCount < numSamples) {
        sb.append("  ").append(n.getId()).append(": ");
        sb.append(n.getPropertyKeys()).append("\n");
        if (propsToPrint != null && propsToPrint.length > 0) {
          String propValuesStr = toString(n, "\n    ", propsToPrint);
          if (propValuesStr.length() > 0)
            sb.append("    ").append(propValuesStr).append("\n");
        }
      }
    }
    sb.append(" Edges:\n");
    int edgeCount = 0;
    for (Edge e : graph.getEdges()) {
      ++edgeCount;
      if (numSamples < 0 || edgeCount < numSamples) {
        sb.append("  ").append(e.getLabel()).append(" ").append(e.getId()).append(" (");
        sb.append(e.getVertex(Direction.OUT)).append("->").append(e.getVertex(Direction.IN));
        sb.append("): ");
        sb.append(e.getPropertyKeys()).append("\n");
        if (propsToPrint != null && propsToPrint.length > 0) {
          String propValuesStr = toString(e, "\n    ", propsToPrint);
          if (propValuesStr.length() > 0)
            sb.append("    ").append(propValuesStr).append("\n");
        }
      }
    }
    sb.append("(").append(nodeCount).append(" nodes, ").append(edgeCount).append(" edges)");
    return (sb.toString());
  }

  public static String toString(Element n, String delim, String... propsToPrint) {
    StringBuilder sb = new StringBuilder();
    if (propsToPrint != null) {
      if (propsToPrint.length == 0) {
        propsToPrint = (String[]) n.getPropertyKeys().toArray(propsToPrint);
      }
      boolean first = true;
      for (String propKey : propsToPrint) {
        if (n.getProperty(propKey) != null) {
          if (first) {
            first = false;
          } else {
            sb.append(delim);
          }
          sb.append(propKey).append("=").append(n.getProperty(propKey).toString());
        }
      }
    }
    return sb.toString();
  }

  public static String toString(Element n, String delim, boolean printId) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    if (printId) {
      sb.append("id=").append(n.getId());
      first = false;
    }
    for (String propKey : n.getPropertyKeys()) {
      if (n.getProperty(propKey) != null) {
        if (first) {
          first = false;
        } else {
          sb.append(delim);
        }
        sb.append(propKey).append("=").append(n.getProperty(propKey).toString());
      }
    }
    return sb.toString();
  }

  public static String toString(Element n) {
    return "\n\t" + ((n instanceof Edge) ? ((Edge) n).getLabel() + "\n\t" : "")
        + toString(n, "\n\t", true);
  }

  public static String toString(Iterable<? extends Element> elems) {
    StringBuilder sb = new StringBuilder();
    for (Element n : elems) {
      sb.append("\n\t").append(toString(n, "\n\t", true));
    }
    return sb.toString();
  }

}
