package net.deelam.graph;

import java.io.Serializable;
import java.util.Map;

import com.tinkerpop.blueprints.Element;

public interface GrafRecordElement extends Element, Serializable {

  String getStringId();

  boolean equals(Object obj);

  Map<String, Object> getProps();
  
  void clearProperties();

}
