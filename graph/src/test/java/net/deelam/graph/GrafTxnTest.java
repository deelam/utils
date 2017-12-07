package net.deelam.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrafTxnTest {

  protected TransactionalGraph outGraph;

  @Before
  public void setUp() throws Exception {
    GrafTxn.checkTransactionsClosed();
    System.gc(); // addresses problem with NFS files still being held by JVM 
    FileUtils.deleteDirectory(new File("target/transGraph"));
    counter = 0;
    outGraph = new IdGraph<Neo4jGraph>(new Neo4jGraph("target/transGraph"));
    outGraph.addVertex("1");
    System.out.println(BpGrafDebug.count(outGraph));
    //cleanFiles();
  }

  void shutdownGraphs() {
    outGraph.shutdown();
  }

  @After
  public void cleanFiles() throws IOException {
    outGraph.shutdown();
    System.gc(); // addresses problem with NFS files still being held by JVM 
    FileUtils.deleteDirectory(new File("target/transGraph"));
  }

  static private int counter = 1;

  private Integer proc() {
    outGraph.addVertex("_" + (++counter));
    //System.out.println(counter);
    return counter;
  }

  @Test
  public void testExecuteTransaction() {
    int tx = GrafTxn.begin(outGraph);
    proc();
    GrafTxn.commit(tx);
    assertEquals(2, Iterables.size(outGraph.getVertices()));
  }

  @Test
  public void testExecuteTransactionSerially() {
    int tx = GrafTxn.begin(outGraph);
    proc();
    GrafTxn.commit(tx);
    assertEquals(2, Iterables.size(outGraph.getVertices()));

    tx = GrafTxn.begin(outGraph);
    proc();
    GrafTxn.rollback(tx);
    assertEquals(2, Iterables.size(outGraph.getVertices()));

    tx = GrafTxn.begin(outGraph);
    proc();
    GrafTxn.commit(tx);
    assertEquals(3, Iterables.size(outGraph.getVertices()));
  }

  @Test
  public void testNestedExecuteTransaction() {
    int tx = GrafTxn.begin(outGraph);
    testExecuteTransaction();
    proc();
    GrafTxn.commit(tx);
    assertEquals(3, Iterables.size(outGraph.getVertices()));
  }

  @SuppressWarnings("serial")
  static class ExpectedException extends RuntimeException {
  }

  public void testExecuteTransactionFailure() {
    int tx = GrafTxn.begin(outGraph);
    proc();
    if (counter == 1) {
      GrafTxn.rollback(tx);
      throw new ExpectedException();
    }
    GrafTxn.commit(tx);
  }

  @Test
  public void testNestedExecuteTransactionFailure() throws Exception {
    assertEquals(1, Iterables.size(outGraph.getVertices()));
    int tx = GrafTxn.begin(outGraph);
    try {
      testExecuteTransactionFailure();
      fail("Exception expected");
      GrafTxn.commit(tx);
    } catch (ExpectedException re) {
      // expect exception be thrown
      log.info("vertex1={}", outGraph.getVertex("_" + 1));
      GrafTxn.rollback(tx);
      assertEquals(1, Iterables.size(outGraph.getVertices()));
    }
  }

  public void testExecuteTransactionFailureNoExplicitRollback() {
    int tx = GrafTxn.begin(outGraph);
    proc();
    if (counter == 1) {
      throw new RuntimeException(
          "unexpected and uncaught exception, such that rollback() is not called");
    }
    GrafTxn.commit(tx);
  }

  @Test
  public void testNestedExecuteTransactionFailureNoNestedRollback() throws Exception {
    assertEquals(1, Iterables.size(outGraph.getVertices()));
    int tx = GrafTxn.begin(outGraph);
    try {
      testExecuteTransactionFailureNoExplicitRollback();
      fail("RuntimeException expected");
      GrafTxn.commit(tx);
    } catch (RuntimeException re) {
      // expect exception be thrown
      GrafTxn.rollback(tx);
      assertEquals(1, Iterables.size(outGraph.getVertices()));
    }
  }

}
