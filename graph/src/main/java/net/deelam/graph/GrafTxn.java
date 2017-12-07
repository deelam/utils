package net.deelam.graph;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang.mutable.MutableInt;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Each begin() call must have a corresponding commit() or rollback() for all execution paths, including exceptions.
 * 
 * <pre>
 		tryOn(graph, (graph)->{
			doYourGraphOperations(graph);
			doNestedTransaction(graph);  // contains calls to begin(), commit(), and maybe rollback()
			});
 </pre>
 * Or if performing operations that do not fit in a single transaction, use incremental commitIfFull():
 * <pre>
        int tx=begin(graph, 5000);
        try{
            for(Vertex v: graph.getVertices()){
               doYourGraphOperations(v);
               doNestedTransaction(v);  // Note: if rollback() is called, rolls back to last incremental commit()
               commitIfFull(tx); // increments operationCount and calls commit() if meets 5000 threshold.
            }
            commit(tx);
        }catch(ExpectedException re){
            // expect exception be thrown
            rollback(tx);
            throw re;
        }
 </pre>
 * 
 * If doNestedTransaction() throws an uncaught exception (and rollback() may not have been called),
 * this class will handle it properly given the tx parameter.
 * 
 * When any of the nested transactions calls rollback(),
 * graph.rollback() is called in the outer-most transaction when commit() or rollback() is called.
 */
@Slf4j
@NoArgsConstructor
public class GrafTxn {

  protected static final ThreadLocal<TransactionalGraph> graphHolder = new ThreadLocal<TransactionalGraph>();

  protected static final ThreadLocal<MutableInt> nestingCounter = new ThreadLocal<MutableInt>() {
    protected MutableInt initialValue() {
      return new MutableInt(0);
    }
  };

  protected static final ThreadLocal<AtomicBoolean> rollbackCalled = new ThreadLocal<AtomicBoolean>() {
    protected AtomicBoolean initialValue() {
      return new AtomicBoolean(false);
    };
  };

  protected static final ThreadLocal<MutableInt> operationsCounter = new ThreadLocal<MutableInt>() {
    protected MutableInt initialValue() {
      return new MutableInt(0);
    }
  };

  protected static final ThreadLocal<Integer> commitThreshold = new ThreadLocal<Integer>() {
    protected Integer initialValue() {
      return Integer.valueOf(-1);
    }
  };

  /**
   * @return depth of transaction.
   */
  public static int begin(TransactionalGraph currGraph) {
    checkNotNull(currGraph);
    TransactionalGraph g = graphHolder.get();
    //log.trace("begin("+currGraph);
    if (g == null) {
      currGraph.commit(); // commit any previous transaction that may have been automatically started (e.g., due to read-only operations)
      graphHolder.set(currGraph);
      nestingCounter.get().increment();
      //log.trace("Starting new outer-most transaction on "+currGraph);
      return nestingCounter.get().intValue();
    } else if (g == currGraph) { // intentionally not using equals()
      nestingCounter.get().increment();
      return nestingCounter.get().intValue();
    } else if (BpGrafUtils.isWrappedWithin(g, currGraph)) {
      nestingCounter.get().increment();
      return nestingCounter.get().intValue();
    } else {
      log.error("This transaction only supports working with one graph ({}) at a time: {}\n"
          + "   Or maybe you forgot to commit() or rollback() the previous transaction?", g,
          currGraph);
      throw new IllegalArgumentException("This transaction only supports working with one graph at a time: "
          + "Expecting " + g + " but got " + currGraph
          + ".  The second graph is not equal to or nested in the first graph.  "
          + "Also make sure there is a commit() for every begin().");
    }
  }

  /**
   * @return if top-level transaction was closed without rollback. False means (1) transaction was nested and is still open, or (2) rollback was called.
   */
  public static boolean commit(int tx) {
    decrementNestingDepth(tx);

    if (isOuterMostTransaction()) {
      return endTransaction();
    } else {
      // log.debug("Depth={}, Not yet committing transaction on graph: {}", nestingCounter.get(), graphHolder.get());
      return false;
    }
  }

  /**
   * @return depth of transaction.
   */
  public static int begin(TransactionalGraph currGraph, int threshold) {
    if (!isOuterMostTransaction()) {
      throw new IllegalStateException(
          "Must not already be in a transaction since graph.commit() must be repeatedly called.");
    }

    commitThreshold.set(Integer.valueOf(threshold));
    return begin(currGraph);
  }

  public static boolean commitIfFull(int tx) {
    operationsCounter.get().increment();
    if (operationsCounter.get().intValue() >= commitThreshold.get().intValue()) {
      log.info("Threshold {} reached; committing transaction on graph: {}", operationsCounter.get(), graphHolder.get());
      graphHolder.get().commit();
      operationsCounter.get().setValue(0);
      return true;
    }
    return false;
  }

  public static boolean isInTransaction() {
    return graphHolder.get() != null;
  }

  public static boolean isOuterMostTransaction() {
    return nestingCounter.get().intValue() == 0;
  }

  public static void checkTransactionsClosed() {
    checkState(!isInTransaction());
  }

  /**
   * Assumes commit() will not be called for the corresponding begin() associated with this transaction.
   * @return if rollback actually occurred.  False means transaction is nested, and 
   *     rollback is delayed until the top-level transaction is committed or rolledback.
   */
  public static boolean rollback(int tx) {
    return rollback(tx, null);
  }
  public static boolean rollback(int tx, Throwable t) {
    log.warn("Rolling graph transaction back from: {}", tx);
    log.warn("Rolling back", t);
    decrementNestingDepth(tx);

    if (!rollbackCalled.get().get()) {
      rollbackCalled.get().set(true);
    }

    if (isOuterMostTransaction()) {
      endTransaction();
      return true;
    } else {
      return false;
    }
  }

  ///

  private static void decrementNestingDepth(int depth) {
    if (depth < nestingCounter.get().intValue()) {
      log.warn("Transaction depth mismatch: expected {} but got {}.  "
          + "This can occur if you called rollback.  Readjusting depth.\n"
          + "   Did you forget to catch an exception and call rollback?",
          nestingCounter.get()
              .intValue(),
          depth);
      nestingCounter.get().setValue(depth - 1);
    } else {
      MutableInt nestingDepth = nestingCounter.get();
      if (nestingDepth.intValue() == 0)
        log.warn("Not decrementing transaction depth since it's already 0.  "
            + "Should investigate stacktrace for the reason:",
            new Throwable());
      else
        nestingDepth.decrement();
    }
  }

  private static boolean endTransaction() {
    /// remove all ThreadLocal variables so they can be GC'd
    nestingCounter.remove();
    commitThreshold.remove();
    if (rollbackCalled.get().get()) {
      rollbackCalled.remove();
      log.warn("Rolling back outer-most transaction on graph: {}", graphHolder.get());
      if (graphHolder.get() != null) {
        graphHolder.get().rollback();
        graphHolder.remove();
      }
      return false;
    } else {
      rollbackCalled.remove();
      log.debug("Committing outer-most transaction on graph: {}", graphHolder.get());
      graphHolder.get().commit();
      graphHolder.remove();
      return true;
    }
  }

  ///

  public static <T extends TransactionalGraph> void tryOn(T graph, Consumer<T> consumer) {
    int tx = begin(graph);
    try {
      consumer.accept(graph);
      commit(tx);
    } catch (Throwable e) {
      rollback(tx, e);
      throw e;
    }
  }

  public static <T extends TransactionalGraph> void tryCAndCloseTxn(T graph, Consumer<T> consumer) {
    tryAndCloseTxn(graph, consumer);
  }
  public static <T extends TransactionalGraph> void tryAndCloseTxn(T graph, Consumer<T> consumer) {
    try {
      tryOn(graph, consumer);
    } finally {
      checkTransactionsClosed();
    }
  }

  public static <T extends TransactionalGraph> void tryOn(T graph, Runnable consumer) {
    int tx = begin(graph);
    try {
      consumer.run();
      commit(tx);
    } catch (Throwable e) {
      rollback(tx, e);
      throw e;
    }
  }

  public static <T extends TransactionalGraph> void tryAndCloseTxn(T graph, Runnable runnable) {
    try {
      tryOn(graph, runnable);
    } finally {
      checkTransactionsClosed();
    }
  }

  public static <T extends TransactionalGraph, R> R tryOn(T graph, Function<T, R> fn) {
    int tx = begin(graph);
    try {
      R result = fn.apply(graph);
      commit(tx);
      return result;
    } catch (Throwable e) {
      rollback(tx, e);
      throw e;
    }
  }

  public static <T extends TransactionalGraph, R> R tryFAndCloseTxn(T graph, Function<T, R> fn) {
    return tryAndCloseTxn(graph, fn);
  }

  public static <T extends TransactionalGraph, R> R tryAndCloseTxn(T graph, Function<T, R> fn) {
    try {
      return tryOn(graph, fn);
    } finally {
      checkTransactionsClosed();
    }
  }

  public static void tryVerticesAndCloseTxn(TransactionalGraph graph, int increment, Consumer<Vertex> consumer) {
    int tx = begin(graph, increment);
    try {
      for (Vertex v : graph.getVertices()) {
        consumer.accept(v);
        commitIfFull(tx);
      }
      commit(tx);
    } catch (Throwable e) {
      rollback(tx, e);
      throw e;
    } finally {
      checkTransactionsClosed();
    }
  }

  public static void tryEdgesAndCloseTxn(TransactionalGraph graph, int increment, Consumer<Edge> consumer) {
    int tx = begin(graph, increment);
    try {
      for (Edge v : graph.getEdges()) {
        consumer.accept(v);
        commitIfFull(tx);
      }
      commit(tx);
    } catch (Throwable e) {
      rollback(tx, e);
      throw e;
    } finally {
      checkTransactionsClosed();
    }
  }

}
