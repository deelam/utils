package net.deelam.graph;

import java.util.Date;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class GrafMetadataNode {

  public static final String METADATA_VERTEXID = "_GRAPH_METADATA_";
  public static final String GRAPH_METADATA_PROP = "_GRAPH_METADATA_";
  private static final String TIMESTAMP_PROP = "_TIMESTAMP_";
  public static final String GRAPHURI_PROP = "_GRAPHURI_";
  public static final String GRAPHBUILDER_PROPKEY = "_GRAPHBUILDER_PROPKEY_";

  //  private static final String VERTEXTYPES_PROP = "_VERTEXTYPES_";
  //  private static final String EDGELABELS_PROP = "_EDGELABELS_";

  public static Vertex getMetaDataNode(IdGraph<?> graph) {
    Vertex mdV = graph.getVertex(GrafMetadataNode.METADATA_VERTEXID);
    return mdV;
  }

  public static Vertex setMetaData(IdGraph<?> graph, String propKey, Object propValue) {
    Vertex mdV = getMetaDataNode(graph);
    mdV.setProperty(propKey, propValue);
    return mdV;
  }

  public static <T> T getMetaData(IdGraph<?> graph, String propKey) {
    Vertex mdV = getMetaDataNode(graph);
    return mdV.getProperty(propKey);
  }

  public static void addMetaDataNode(GrafUri gUri, IdGraph<?> graph) {
    Vertex mdV = getMetaDataNode(graph);
    if (mdV == null) {
      //log.info("Adding METADATA_VERTEX with id={} {}", METADATA_VERTEXID, toString(graph));
      mdV = graph.addVertex(GrafMetadataNode.METADATA_VERTEXID);
      mdV.setProperty(GRAPH_METADATA_PROP, true);
      mdV.setProperty(TIMESTAMP_PROP, new Date().toString());
      mdV.setProperty(GRAPHURI_PROP, gUri.asString());
      //      mdV.setProperty(VERTEXTYPES_PROP, gUri.getVertexTypes());
      //      mdV.setProperty(EDGELABELS_PROP, gUri.getEdgeLabels());
      graph.commit();
      //log.info("Added METADATA_VERTEX to {}", toString(graph));
    }
  }
}
