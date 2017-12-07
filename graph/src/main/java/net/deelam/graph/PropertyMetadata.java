package net.deelam.graph;

import com.tinkerpop.blueprints.Element;

import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(fluent = true, chain = true)
@Data
public class PropertyMetadata {
  int numValues = -1;

  enum SINGLEVALUED_STRATEGY {
    KEEP_FIRST, KEEP_LAST
  }

  SINGLEVALUED_STRATEGY singleStrat = null;

  static PropertyMetadata createSingleValued(SINGLEVALUED_STRATEGY svStrat) {
    return new PropertyMetadata().numValues(1).singleStrat(svStrat);
  }

  static PropertyMetadata createSingleValued() {
    return createSingleValued(SINGLEVALUED_STRATEGY.KEEP_FIRST);
  }

  enum MULTIVALUED_COL_TYPE {
    SET, LIST
  }

  MULTIVALUED_COL_TYPE collectionType = null;

  static PropertyMetadata createSetValued() {
    return new PropertyMetadata().numValues(-1).collectionType(MULTIVALUED_COL_TYPE.SET);
  }

  enum MULTIVALUED_STRATEGY {
    PREPEND, APPEND
  }

  MULTIVALUED_STRATEGY addStrat = null;

  static PropertyMetadata createListValued(MULTIVALUED_STRATEGY addStrat) {
    return new PropertyMetadata().numValues(-1).collectionType(MULTIVALUED_COL_TYPE.LIST)
        .addStrat(addStrat);
  }

  static PropertyMetadata createListValued() {
    return createListValued(MULTIVALUED_STRATEGY.APPEND);
  }

  @FunctionalInterface
  public static interface MergeFunction {
    boolean merge(PropertyMetadata pmd, String key, Object fromValue, Element toE);
  }

  MergeFunction mergeF;
}
