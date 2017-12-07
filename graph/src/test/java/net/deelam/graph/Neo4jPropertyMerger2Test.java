package net.deelam.graph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class Neo4jPropertyMerger2Test {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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
  public void testNeo4jPM() throws IOException {
    gUri = new GrafUri("neo4j:///./target/propMerger2");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    Neo4jPropertyMerger2 pm = new Neo4jPropertyMerger2();

    String propKey = "prop";
    {
      PropertyMetadata pmd = PropertyMetadata
          .createSetValued();
      //          .createListValued(PropertyMetadata.MULTIVALUED_STRATEGY.PREPEND);
      //.createSingleValued(PropertyMetadata.SINGLEVALUED_STRATEGY.KEEP_FIRST);
      pm.putPropertyMetadata(propKey, pmd);
    }
    testPropMerger(graph, pm, 0, propKey);

    {
      Element toE = graph.getVertex("toV");
      Element toE2 = graph.getVertex("toV2");
      // Set+Set -> Set

      pm.mergeProperties(toE2, toE);
      //      assertEquals(2+extraProps, toE.getPropertyKeys().size());
      //assertEquals(SET_VALUE, toE.getProperty(PROPKEY));
      assertEquals(2, pm.getListProperty(toE, propKey).size());
      assertEquals(2, pm.getListPropertySize(toE, propKey));
      //      System.out.println("3: "+toE2.getProperty(PROPKEY));

      // test duplicate value doesn't change valueset
      pm.addProperty(toE2, propKey, "2");
      //      assertEquals(2+extraProps, toE2.getPropertyKeys().size());
      assertEquals(2, pm.getListProperty(toE2, propKey).size());
      assertEquals(2, pm.getListPropertySize(toE2, propKey));
    }

    Element toE2 = graph.getVertex("toV2");
    assertEquals(2, ((List) toE2.getProperty(propKey)).size());
    assertEquals(String.class, ((Collection) toE2.getProperty(propKey)).iterator().next().getClass());

    Element toE = graph.getVertex("toV");
    assertEquals(2, ((List) toE.getProperty(propKey)).size());
    assertEquals(String.class, ((List) toE.getProperty(propKey)).get(0).getClass());
  }

  @Test
  public void testNeo4jPMList() throws IOException {
    gUri = new GrafUri("neo4j:///./target/propMerger2List");
    IdGraph<TinkerGraph> graph = gUri.createNewIdGraph(true);
    Neo4jPropertyMerger2 pm = new Neo4jPropertyMerger2();

    String propKey = "prop" + PropertyMerger.VALUELIST_SUFFIX;
    {
      PropertyMetadata listPmd = PropertyMetadata
          .createListValued();
      //          .createListValued(PropertyMetadata.MULTIVALUED_STRATEGY.PREPEND);
      //.createSingleValued(PropertyMetadata.SINGLEVALUED_STRATEGY.KEEP_FIRST);
      pm.putPropertyMetadata(propKey, listPmd);

    }
    testPropMerger(graph, pm, 0, propKey);

    {
      Element toE = graph.getVertex("toV");
      Element toE2 = graph.getVertex("toV2");
      // Set+Set -> Set

      pm.mergeProperties(toE2, toE);
      //      assertEquals(2+extraProps, toE.getPropertyKeys().size());
      //assertEquals(SET_VALUE, toE.getProperty(PROPKEY));
      assertEquals(5, pm.getListProperty(toE, propKey).size());
      assertEquals(5, pm.getListPropertySize(toE, propKey));
      System.out.println("2z: " + toE.getProperty(propKey));
      System.out.println("3: " + toE2.getProperty(propKey));

      // test duplicate value doesn't change valueset
      pm.addProperty(toE2, propKey, "2");
      System.out.println("3b: " + toE2.getProperty(propKey));
      //      assertEquals(2+extraProps, toE2.getPropertyKeys().size());
      assertEquals(3, pm.getListProperty(toE2, propKey).size());
      assertEquals(3, pm.getListPropertySize(toE2, propKey));
    }

    Element toE2 = graph.getVertex("toV2");
    assertEquals(3, ((List) toE2.getProperty(propKey)).size());
    assertEquals(String.class, ((Collection) toE2.getProperty(propKey)).iterator().next().getClass());

    Element toE = graph.getVertex("toV");
    assertEquals(5, ((List) toE.getProperty(propKey)).size());
    assertEquals(String.class, ((List) toE.getProperty(propKey)).get(0).getClass());
  }

  private void testPropMerger(IdGraph<?> graph, PropertyMerger pm, int extraProps, String propkey) throws IOException {
    //  System.out.println("pm="+pm);
    Vertex fromE = graph.addVertex("fromV");
    fromE.setProperty(propkey, "1");
    Element toE = graph.addVertex("toV");
    pm.mergeProperties(fromE, toE);
    //  System.out.println("0: "+toE.getProperty(propkey)+" "+toE.getProperty(propkey).getClass());

    assertEquals(1, toE.getPropertyKeys().size());
    assertEquals("1", toE.getProperty(propkey));

    Element toE2 = graph.addVertex("toV2");
    pm.mergeProperties(toE, toE2);

    assertEquals(1, toE2.getPropertyKeys().size());
    assertEquals("1", toE2.getProperty(propkey));

    pm.mergeProperties(toE2, fromE);
    assertEquals(1, fromE.getPropertyKeys().size());
    assertEquals(1, pm.getListProperty(toE2, propkey).size());
    assertEquals("1", fromE.getProperty(propkey));

    // val+val -> Set

    toE2.setProperty(propkey, "2");
    pm.mergeProperties(toE, toE2);
    //assertEquals(2+extraProps, toE2.getPropertyKeys().size());
    //assertEquals(SET_VALUE, toE.getProperty(propkey));
    //  System.out.println("1: "+toE2.getProperty(propkey));
    assertEquals(2, pm.getListProperty(toE2, propkey).size());
    assertEquals(String.class, pm.getListProperty(toE2, propkey).get(0).getClass());
    assertEquals("2", pm.getListProperty(toE2, propkey).get(0));
    assertEquals(2, pm.getListPropertySize(toE2, propkey));

    // Set+val -> Set

    pm.mergeProperties(toE2, toE);
    //assertEquals(2+extraProps, toE.getPropertyKeys().size());
    //assertEquals(SET_VALUE, toE.getProperty(propkey));
    //  System.out.println("2: "+toE2.getProperty(propkey));
  }
}
