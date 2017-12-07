package net.deelam.graph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graph.BpGrafIndexProps.PropertyKeys;

@Accessors(fluent = true)
@ToString
@Slf4j
public class GrafUri {

  public String asString() {
    return getFactory().asString(this);
  }

  String origUri;

  @Override
  public boolean equals(Object arg) {
    if (arg instanceof GrafUri)
      return origUri.equals(((GrafUri) arg).origUri);
    return false;
  }

  @Override
  public int hashCode() {
    return origUri.hashCode();
  }

  public GrafUri(String uri) {
    this(uri, new BaseConfiguration());
  }

  @Getter
  private final String scheme;
  @Getter
  private final URI subUri;
  @Getter
  private final String uriPath;
  @Getter
  private final Configuration config;

  public GrafUri set(String prop, Object val) {
    config.setProperty(prop, val);
    return this;
  }

  public GrafUri(String graphuri, Configuration config) {
    URI.create(graphuri);
    Preconditions.checkNotNull(graphuri, "uri parameter cannot be null");
    origUri = graphuri;
    this.config = config;
    int colonIndx = graphuri.indexOf(':');
    Preconditions.checkState(colonIndx > 0, "Expecting something like 'tinker:' but got " + graphuri);
    scheme = graphuri.substring(0, colonIndx);
    subUri = URI.create(graphuri.substring(colonIndx + 1));
    uriPath = parseUriPath(subUri);
    parseQuery(subUri.toString());

    getFactory().init(this);
  }

  @Getter
  private boolean readOnly = false;

  @Setter
  private Consumer<IdGraph<?>> openHook = null;

  public boolean exists() {
    return getFactory().exists(this);
  }

  public boolean delete() throws IOException {
    checkNotOpen();
    if (getFactory().exists(this)) {
      getFactory().delete(this);
      return true;
    } else {
      return false;
    }
  }

  public boolean isOpen() {
    return graph != null;
  }

  private void checkNotOpen() {
    if (isOpen())
      throw new RuntimeException("Graph is open: {}" + graph);
  }

  @SuppressWarnings("rawtypes")
  @Getter
  private IdGraph graph;

  public void shutdown() {
    if (isOpen()) {
      log.info("Shutting down graph={}", graph);
      try {
        getFactory().shutdown(this);
        log.info("  Shut down graphUri={}", this);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      graph = null;
    } else {
      log.warn("Cannot shutdown; graph is not opened or you didn't open the graph "
          + "using this GraphUri instance: " + origUri,
          new Throwable("Check this call stack"));
    }
  }

  ///

  @SuppressWarnings("rawtypes")
  public IdGraph openExistingIdGraph() throws FileNotFoundException {
    checkNotOpen();
    if (exists()) {
      return openIdGraph();
    } else {
      throw new FileNotFoundException("Graph not found at " + uriPath);
    }
  }

  /**
   * Create a new empty graph, deleting any existing graph if deleteExisting=true
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public <T extends KeyIndexableGraph> IdGraph<T> createNewIdGraph(boolean deleteExisting) throws IOException {
    checkNotOpen();
    if (exists()) {
      if (deleteExisting)
        delete();
      else
        throw new FileAlreadyExistsException("Graph exists at: " + uriPath);
    }
    return openIdGraph();
  }

  public static final String CREATE_META_DATA_NODE = "createMetaDataNode";

  /**
   * Open existing or create a new graph
   * @param baseGraphClass
   * @return
   */
  @SuppressWarnings("rawtypes")
  public IdGraph openIdGraph() {
    checkNotOpen();
    log.info("Opening graphUri={} using {}", this, getFactory());
    //printConfig(config);
    try {
      graph = getFactory().open(this);

      if (!readOnly) {
        boolean createMetaDataNode = config.getBoolean(CREATE_META_DATA_NODE, true);
        log.debug("  Opened graph={}, createMetaDataNode={}", graph, createMetaDataNode);
        if (createMetaDataNode)
          GrafMetadataNode.addMetaDataNode(this, graph);
      }

      if (openHook != null)
        openHook.accept(graph);
    } catch (RuntimeException re) {
      log.error("Could not open graphUri=" + this, re);
      throw re;
    }
    return graph;
  }

  ///

  private String parseUriPath(URI uri) {
    String path = uri.getPath();
    if (path == null) {
      if (uri.getScheme() == null)
        return null;
      else { // try to get path after removing scheme
        String ssp = uri.getSchemeSpecificPart();
        path = URI.create(ssp).getPath();
        if (path == null)
          return null;
      }
    }
    Preconditions.checkNotNull(path);
    // check if path is relative
    if (path.length() > 1 && path.startsWith("/."))
      path = path.substring(1); // remove first char, which is '/'
    return path;
  }

  private void parseQuery(String uriStr) {
    int queryIndx = uriStr.indexOf('?');
    if (queryIndx < 0)
      return;
    String queryStr = uriStr.substring(queryIndx + 1);
    if (queryStr != null) {
      for (String kv : queryStr.split("&")) {
        if (kv.length() > 0) {
          String[] pair = kv.split("=");
          if (config.containsKey(pair[0]))
            log.warn("Overriding configuration {}={} with {}", pair[0], config.getProperty(pair[0]), pair[1]);
          config.setProperty(pair[0], pair[1]);
        }
      }
    }
  }

  ///

  public void backupTo(GrafUri dstGraphUri) throws IOException {
    if (this.isOpen())
      throw new IllegalStateException("Source graph must not be open so underlying files can be copied.");
    if (dstGraphUri.isOpen())
      throw new IllegalStateException("Destination graph must not be open so underlying files can be copied.");
    if (dstGraphUri.exists())
      throw new IllegalStateException("Destination graph must not already exist so underlying files can be copied.");
    getFactory().backup(this, dstGraphUri);
  }

  public PropertyMerger createPropertyMerger() {
    return getFactory().createPropertyMerger();
  }

  public void createIndices(PropertyKeys pks) {
    if (!this.isOpen())
      throw new IllegalStateException("Graph must not be open to create indices.");
    getFactory().createIndices(this, pks);
  }


  ///

  private static Map<String, IdGrafFactory> graphFtry = new HashMap<>(5);

  public static void register(String scheme, IdGrafFactory factory) {
    IdGrafFactory oldFactory = graphFtry.put(scheme, factory);
    if (oldFactory == null)
      log.info("Registering scheme={} with {}", scheme, factory);
    else
      log.info("Replacing existing={} for scheme={} with {}", oldFactory, scheme, factory);
  }

  public static void register(IdGrafFactory factory) {
    register(factory.getScheme(), factory);
  }

  private IdGrafFactory getFactory() {
    IdGrafFactory factory = graphFtry.get(scheme);
    Preconditions.checkNotNull(factory, "Unknown schema: " + scheme);
    return factory;
  }

  ///

  @SuppressWarnings("unchecked")
  public static void printConfig(Configuration config) {
    StringBuilder sb = new StringBuilder();
    for (Iterator<String> itr = config.getKeys(); itr.hasNext();) {
      String key = itr.next();
      sb.append("\n").append(key).append("=").append(config.getString(key));
    }
    log.info("config={}", sb);
  }
}
