package net.deelam.graph;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@SuppressWarnings("unchecked")
public class GrafUriTest {

  @BeforeClass
  public static void setUp() throws Exception {
    IdGrafFactoryTinker.register();
    //		new File("testGraphs").mkdir();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    //		FileUtils.deleteDirectory(new File("testGraphs"));
  }

  @Test
  public void testInMemGraph() throws IOException {
    GrafUri gUri = new GrafUri("tinker:///");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testInMemGraph2() throws IOException {
    GrafUri gUri = new GrafUri("tinker:/");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }
  
  @Test
  public void testSavedGraph0() {
    GrafUri gUri = new GrafUri("tinker:./target/tGraph");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }
  
  @Test
  public void testSavedGraph1() throws IOException {
    GrafUri gUri = new GrafUri("tinker:///./target/tGraph");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testSavedGraph2() throws IOException {
    GrafUri gUri = new GrafUri("tinker:"+new java.io.File("target","tGraph").getAbsolutePath());
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }
  
  @Test
  public void testSavedGraphml() {
    GrafUri gUri = new GrafUri("tinker:./target/tGraphML?fileType=graphml");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }

  @Test
  public void testSavedGraphmlScheme() {
    GrafUri gUri = new GrafUri("tinker:graphml:./target/tGraphML");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }
  
  @Test
  public void testTwoSavedGraphs() {
    GrafUri gUri = new GrafUri("tinker:./target/tGraphML?fileType=graphml");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();

    GrafUri gUri2 = new GrafUri("tinker:./target/tGraphML2?fileType=graphml");
    IdGraph<TinkerGraph> graph2 = gUri2.openIdGraph();

    graph.shutdown();
    graph2.shutdown();
  }

  @Test
  public void testUriReuse() throws IOException {
    FileUtils.deleteDirectory(new File("target/tGraphMLReuse")); // make sure graph doesn't exist

    GrafUri gUri = new GrafUri("tinker:///./target/tGraphMLReuse?fileType=graphml");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);

    try {
      gUri.openIdGraph(); // should not be able to access new graph
      Assert.fail();
    } catch (RuntimeException re) {
    }
    graph.shutdown();
  }
}
