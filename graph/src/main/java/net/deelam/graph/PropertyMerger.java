package net.deelam.graph;

import java.util.List;

import com.tinkerpop.blueprints.Element;

public interface PropertyMerger {
  static final String VALUELIST_SUFFIX = "$L";

  // TODO: 9: add unit tests for different cases and different classes (eg., Date)
  void mergeProperties(Element fromE, Element toE);

  <T> List<T> getListProperty(Element elem, String key);

  int getListPropertySize(Element elem, String key);

  boolean addProperty(Element elem, String key, Object value);

  boolean isMultivalued(Object value);

}
