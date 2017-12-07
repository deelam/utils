package net.deelam.graph;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public interface GrafRecord extends Vertex, GrafRecordElement, Serializable {

  public static abstract class Factory {
    public abstract GrafRecord create(String id);

    public abstract GrafRecord create(String id, String nodeType);

    public abstract GrafRecordEdge createEdge(String id, String label, String outVertex, String inVertex);

    public GrafRecordEdge createEdge(String id, String label, Vertex outVertex, Vertex inVertex) {
      return createEdge(id, label, (String) outVertex.getId(), (String) inVertex.getId());
    }

    public GrafRecordEdge createEdge(String label, Vertex outVertex, Vertex inVertex) {
      return createEdge(newEdgeId(), label, (String) outVertex.getId(), (String) inVertex.getId());
    }

    protected String newEdgeId() {
      //return "edge" + counter++;
      return UUID.randomUUID().toString();
    }
  }

  String getType();

  Map<String, Edge> getInEdges();

  GrafRecordEdge getInEdge(String edgeId);

  Map<String, Edge> getOutEdges();

  GrafRecordEdge getOutEdge(String edgeId);

  void addEdge(GrafRecordEdge edge);

  void setLongId(long id);

  Long getLongId();

}
