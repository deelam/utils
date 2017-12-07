package net.deelam.graph;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.FramedTransactionalGraph;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import com.tinkerpop.frames.modules.typedgraph.TypedGraphModuleBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FramedGrafSupplier {

  private final FramedGraphFactory factory;

  private Map<String, Class<?>> frameClassesMap = new HashMap<>();

  public FramedGrafSupplier(Class<?>[] typedClasses) {
    TypedGraphModuleBuilder tgmb = new TypedGraphModuleBuilder();
    for (Class<?> clazz : typedClasses) {
      tgmb = tgmb.withClass(clazz);
      registerFrameClass(clazz);
    }
    log.debug("Registered Frame class types: {}", frameClassesMap.keySet());

    factory = new FramedGraphFactory(
        new JavaHandlerModule(), // required to activate @JavaHandler annotations
        tgmb.build() // required to store type info
    );
  }

  public Class<?> getFrameClassOf(Vertex v, String typePropKey) {
    final String vertexType = v.getProperty(typePropKey);
    if (vertexType == null) {
      log.error("getVertexType() returned null.  Does Frame class for vertex {} have a @TypeValue annotation?", v);
    }

    final Class<?> ret = frameClassesMap.get(vertexType);
    if (ret == null) {
      log.error("Could not find class for " + v + ".  Has it been registered?");
    }
    return ret;
  }

  public Class<?> getFrameClassOf(VertexFrame v, String typePropKey) {
    return getFrameClassOf(v.asVertex(), typePropKey);
  }

  private void registerFrameClass(Class<?> clazz) {
    try {
      String typeStr = (String) clazz.getField("TYPE_VALUE").get(clazz);
      checkNotNull(typeStr, "Class must have String field TYPE_VALUE");
      frameClassesMap.put(typeStr, clazz);
    } catch (Exception e) {
      log.warn("Class must have String field TYPE_VALUE", e);
    }
  }

  public FramedTransactionalGraph<TransactionalGraph> get(TransactionalGraph graph) {
    FramedTransactionalGraph<TransactionalGraph> framedGraph = factory.create(graph); // wrap the base graph
    return framedGraph;
  }

}
