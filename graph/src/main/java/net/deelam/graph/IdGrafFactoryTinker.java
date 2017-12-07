package net.deelam.graph;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph.FileType;
import com.tinkerpop.blueprints.util.GraphHelper;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IdGrafFactoryTinker implements IdGrafFactory {

  public static void register() {
    GrafUri.register(new IdGrafFactoryTinker());
  }

  @Getter
  String scheme="tinker";

  @Override
  @SuppressWarnings("unchecked")
  public <T extends KeyIndexableGraph> IdGraph<T> open(GrafUri gUri) {
    // check desired output format
    FileType fileType = getFileSaveType(gUri);

    // open graph
    if (fileType == null) {
      log.debug("Opening Tinker graph in memory");
      return (IdGraph<T>) new IdGraph<>(new TinkerGraph());
    } else {
      String path = gUri.uriPath();
      log.debug("Opening Tinker graph at path={} of type={}", path, fileType);
      return (IdGraph<T>) new IdGraph<>(new TinkerGraph(path, fileType));
    }
  }

  @Override
  public void shutdown(GrafUri gUri) throws IOException {
    gUri.graph().shutdown();

    FileType fileSaveType = getFileSaveType(gUri);
    if (fileSaveType != null)
      switch (fileSaveType) {
        case GRAPHML:
          String graphmlFile = gUri.uriPath() + "/tinkergraph.xml";
          PrettyPrintXml.prettyPrint(graphmlFile, gUri.uriPath() + ".graphml");
          break;
        default:
          log.error("Unknown save format: " + fileSaveType + " for " + gUri);
      }
  }
  
  private boolean isInMemoryGraph(GrafUri gUri) {
    FileType fileType = getFileSaveType(gUri);
    return fileType == null;
  }

  private FileType getFileSaveType(GrafUri gUri) {
    // check for secondary scheme
    String fileTypeStr = gUri.subUri().getScheme(); // enables "tinker:graphml:./target/tGraphML"s

    if (fileTypeStr == null) {
      String path = gUri.uriPath();
      if (path == null || path.length() < 1 || path.equals("/")) { 
        // in-memory
        return null;
      }
      // check config setting
      fileTypeStr = gUri.config().getString("fileType");
    }

    // not in-memory; storing to disk
    if (fileTypeStr == null) {
      return TinkerGraph.FileType.JAVA; // default type
    } else {
      if (fileTypeStr.equalsIgnoreCase("prettyGraphml")) {
        return TinkerGraph.FileType.GRAPHML;
      } else {
        return TinkerGraph.FileType.valueOf(fileTypeStr.toUpperCase());
      }
    }
  }

  @Override
  public void delete(GrafUri gUri) throws IOException {
    if (isInMemoryGraph(gUri))
      return;
    File pathFile = new File(gUri.uriPath());
    log.info("Deleting TinkerGraph at {}", pathFile);
    FileUtils.deleteDirectory(pathFile);
  }

  @Override
  public void backup(GrafUri srcGraphUri, GrafUri dstGraphUri) throws IOException {
    if (isInMemoryGraph(srcGraphUri)) {
      dstGraphUri.createNewIdGraph(false); // to be safe, don't overwrite dst
      GraphHelper.copyGraph(srcGraphUri.graph(), dstGraphUri.graph());
    } else {
      File srcFile = new File(srcGraphUri.uriPath());
      File destFile = new File(dstGraphUri.uriPath());
      FileUtils.copyDirectory(srcFile, destFile);
    }
  }

  @Override
  public boolean exists(GrafUri gUri) {
    if (isInMemoryGraph(gUri))
      return false;
    File pathFile = new File(gUri.uriPath());
    return pathFile.exists();
  }
  
  @Override
  public PropertyMerger createPropertyMerger() {
    return new JavaSetPropertyMerger();
  }
}
