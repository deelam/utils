package net.deelam.graph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class PropertyMergerTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    IdGrafFactoryTinker.register();
    IdGrafFactoryNeo4j.register();
  }

  GrafUri gUri;

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    gUri.shutdown();
    gUri.delete();
  }

  @Test
  public void testJavaSetPM() throws IOException {
    gUri = new GrafUri("tinker:/");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    JavaSetPropertyMerger pm = new JavaSetPropertyMerger();
    testPropMerger(graph, pm, 0);

    Element toE2 = graph.getVertex("toV2");
    assertEquals(2, ((Set) toE2.getProperty("prop")).size());
    assertEquals(String.class, ((Set) toE2.getProperty("prop")).iterator().next().getClass());

    Element toE = graph.getVertex("toV");
    assertEquals(2, ((Set) toE.getProperty("prop")).size());
    assertEquals(String.class, ((Set) toE.getProperty("prop")).iterator().next().getClass());
  }

  @Test
  public void testNeo4jPM() throws IOException {
    gUri = new GrafUri("neo4j:///./target/propMerger");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    Neo4jPropertyMerger pm = new Neo4jPropertyMerger();
    testPropMerger(graph, pm, 0);

    Element toE2 = graph.getVertex("toV2");
    assertEquals(2, ((List) toE2.getProperty("prop")).size());
    assertEquals(String.class, ((Collection) toE2.getProperty("prop")).iterator().next().getClass());

    Element toE = graph.getVertex("toV");
    assertEquals(2, ((List) toE.getProperty("prop")).size());
    assertEquals(String.class, ((List) toE.getProperty("prop")).get(0).getClass());
  }

  @Test
  public void testNeo4jPMWithTinkerGraph() throws IOException {
    gUri = new GrafUri("tinker:/");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    Neo4jPropertyMerger pm = new Neo4jPropertyMerger();
    testPropMerger(graph, pm, 0);

    Element toE2 = graph.getVertex("toV2");
    assertEquals(2, ((Object[]) toE2.getProperty("prop")).length);
    assertEquals(String.class, ((Object[]) toE2.getProperty("prop"))[0].getClass());

    Element toE = graph.getVertex("toV");
    assertEquals(2, ((Object[]) toE.getProperty("prop")).length);
    assertEquals(String.class, ((Object[]) toE.getProperty("prop"))[0].getClass());
  }

  @Test
  public void testJsonPM() throws IOException {
    gUri = new GrafUri("tinker:/");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    PropertyMerger pm = new JsonPropertyMerger();
    testPropMerger(graph, pm, 1);

    Element toE2 = graph.getVertex("toV2");
    assertEquals("[\"2\",\"1\"]", toE2.getProperty("prop"));
  }

  @Test
  public void testJsonPMWithNeo4jGraph() throws IOException {
    gUri = new GrafUri("neo4j:///./target/propMerger");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    PropertyMerger pm = new JsonPropertyMerger();
    testPropMerger(graph, pm, 1);

    Element toE2 = graph.getVertex("toV2");
    assertEquals("[\"2\",\"1\"]", toE2.getProperty("prop"));
  }

  private void testPropMerger(IdGraph<?> graph, PropertyMerger pm, int extraProps) throws IOException {
    //    System.out.println("pm="+pm);
    Vertex fromE = graph.addVertex("fromV");
    fromE.setProperty("prop", "1");
    Element toE = graph.addVertex("toV");
    pm.mergeProperties(fromE, toE);
    //    System.out.println("0: "+toE.getProperty("prop")+" "+toE.getProperty("prop").getClass());

    assertEquals(1, toE.getPropertyKeys().size());
    assertEquals("1", toE.getProperty("prop"));

    Element toE2 = graph.addVertex("toV2");
    pm.mergeProperties(toE, toE2);

    assertEquals(1, toE2.getPropertyKeys().size());
    assertEquals("1", toE2.getProperty("prop"));

    pm.mergeProperties(toE2, fromE);
    assertEquals(1, fromE.getPropertyKeys().size());
    assertEquals(1, pm.getListProperty(toE2, "prop").size());
    assertEquals("1", fromE.getProperty("prop"));

    // val+val -> Set

    toE2.setProperty("prop", "2");
    pm.mergeProperties(toE, toE2);
    //assertEquals(2+extraProps, toE2.getPropertyKeys().size());
    //assertEquals(SET_VALUE, toE.getProperty("prop"));
    //    System.out.println("1: "+toE2.getProperty("prop"));
    assertEquals(2, pm.getListProperty(toE2, "prop").size());
    assertEquals(String.class, pm.getListProperty(toE2, "prop").get(0).getClass());
    assertEquals("2", pm.getListProperty(toE2, "prop").get(0));
    assertEquals(2, pm.getListPropertySize(toE2, "prop"));

    // Set+val -> Set

    pm.mergeProperties(toE2, toE);
    //assertEquals(2+extraProps, toE.getPropertyKeys().size());
    //assertEquals(SET_VALUE, toE.getProperty("prop"));
    //    System.out.println("2: "+toE2.getProperty("prop"));

    // Set+Set -> Set

    pm.mergeProperties(toE2, toE);
    //    assertEquals(2+extraProps, toE.getPropertyKeys().size());
    //assertEquals(SET_VALUE, toE.getProperty("prop"));
    assertEquals(2, pm.getListProperty(toE, "prop").size());
    assertEquals(2, pm.getListPropertySize(toE, "prop"));
    //    System.out.println("3: "+toE2.getProperty("prop"));

    // test duplicate value doesn't change valueset
    pm.addProperty(toE2, "prop", "2");
    //    assertEquals(2+extraProps, toE2.getPropertyKeys().size());
    assertEquals(2, pm.getListProperty(toE2, "prop").size());
    assertEquals(2, pm.getListPropertySize(toE2, "prop"));

  }

}
