package net.deelam.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.iterators.ArrayIterator;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * store primitives or arrays of primitives
 */
@RequiredArgsConstructor
@Slf4j
public class Neo4jPropertyMerger2 extends Neo4jPropertyMerger {

  @Setter
  private Set<String> propertiesToCopy = null;
  final PropertyMetadata.MergeFunction DEFAULT_MERGE_FUNCTION = (pmd, key, fromValue, toE) -> {
    //Object fromValue = fromE.getProperty(key);
    if (fromValue == null) {
      return false;
    }

    if (propertiesToCopy != null && !propertiesToCopy.contains(key))
      return false;

    // fromValue is not null at this point
    Object toValue = toE.getProperty(key);
    if (toValue == null) {
      setElementProperty(toE, key, fromValue);
      return true;
    } else if (!isMultivalued(fromValue) && toValue.equals(fromValue)) {
      // nothing to do; values are the same
      return false;
    } else
      try {
        mergeValues_default(fromValue, toE, key, pmd);
        return true;
      } catch (Exception e) {
        log.warn("Could not merge property values for key=" + key, e);
        return false;
      }
  };

  private static final PropertyMetadata MERGE_FUNC_IGNORE = PropertyMetadata
      .createSingleValued().mergeF((pmd, key, fromE, toE) -> false);

  @Setter
  private PropertyMetadata defaultPMD = PropertyMetadata.createSetValued()
      .mergeF(DEFAULT_MERGE_FUNCTION);

  private Map<String, PropertyMetadata> propMetaMap = new HashMap<>();

  {
    propMetaMap.put(IdGraph.ID, MERGE_FUNC_IGNORE);
  }

  private PropertyMetadata getPropMetadata(String key) {
    PropertyMetadata pMD = propMetaMap.get(key);
    if (pMD == null)
      pMD = defaultPMD;
    return pMD;
  }

  private PropertyMetadata.MergeFunction getMergeFunc(PropertyMetadata pMD) {
    PropertyMetadata.MergeFunction mergeF = pMD.mergeF;
    if (mergeF == null)
      mergeF = defaultPMD.mergeF;
    return mergeF;
  }

  public void putPropertyMetadata(String key, PropertyMetadata pmd) {
    propMetaMap.put(key, pmd);
    if (pmd.numValues > 1)
      allowMultivaluedProperty(key);
  }

  @Override
  public void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if (key.length() == 0)
        throw new IllegalArgumentException("Property key cannot be empty: " + fromE);

      PropertyMetadata pMD = getPropMetadata(key);
      PropertyMetadata.MergeFunction mergeF = getMergeFunc(pMD);
      mergeF.merge(pMD, key, fromE.getProperty(key), toE);
    }
  }

  @Override
  public boolean addProperty(Element elem, String key, Object value) {
    if (isMultivalued(value))
      throw new IllegalArgumentException("TODO: allow adding multivalued value?");
    Object existingVal = elem.getProperty(key);
    if (existingVal == null) {
      elem.setProperty(key, value);
      return true;
    } else {
      //mergeValues(existingVal, elem, key);
      PropertyMetadata pMD = getPropMetadata(key);
      PropertyMetadata.MergeFunction mergeF = getMergeFunc(pMD);
      mergeF.merge(pMD, key, value, elem);
      return false;
    }
  }

  public void mergeValues(Object fromValue, Element toE, String key) {
    PropertyMetadata pMD = getPropMetadata(key);
    PropertyMetadata.MergeFunction mergeF = getMergeFunc(pMD);
    mergeF.merge(pMD, key, fromValue, toE);
    //mergeValues(fromValue, toE, key, pMD);
  }

  private void mergeValues_default(Object fromValue, Element toE, String key, PropertyMetadata pmd) {
    // toValue and fromValue are not null and want to add fromValue to the list
    /* Possible cases:
     * fromValue=val toValue=val     ==> toValue__SET=Set<?>
     * fromValue=Set<?> toValue=val  ==> toValue__SET=Set<?>
     * fromValue=val toValue=Set<?>  ==> toValue__SET=Set<?>
     * fromValue=Set<?> toValue=Set<?>  ==> toValue__SET=Set<?>
     */

    // check special SET_SUFFIX property and create a Set if needed
    Object value = toE.getProperty(key);
    Collection<Object> valueList = null;
    if (isMultivalued(value)) {
      if (pmd.collectionType == null)
        log.warn("Property should not be multivalued: {} {}", key, pmd);
      valueList = tryConvertArrayToCollection(value);
    }

    if (valueList == null) {
      valueList = new ArrayList<>();
      if (!validPropertyClasses.contains(value.getClass()))
        throw new IllegalArgumentException("Not valid: " + value.getClass());
      valueList.add(value);
      if (!key.endsWith(VALUELIST_SUFFIX) && !allowedMultivaluedProps.contains(key)) {
        log.warn("Property has multiple values which is inefficient: key=" + key + " for node=" + toE.getId()
            + " existingVal=" + value + " addedValue=" + fromValue/*, new Throwable("Call stack")*/);
      }
    } else if (maxValueListSize > 0 && valueList.size() > maxValueListSize) {
      log.warn(toE.getId() + " has property=" + key + " with large value list size=" + valueList.size()
          + "  Not adding more values: " + fromValue);
      return;
    }

    /// check if fromValue is already in toValueList
    boolean toValueChanged = false;
    if (isMultivalued(fromValue)) {
      Iterator<?> itr;
      if (fromValue instanceof Collection)
        itr = ((Collection<?>) fromValue).iterator();
      else if (fromValue.getClass().isArray())
        itr = new ArrayIterator(fromValue); // Collection<?> fromValueList = tryConvertArrayToCollection(fromValue);
      else
        throw new IllegalArgumentException("How to iterate on " + fromValue.getClass());
      while (itr.hasNext()) {
        Object fVal = itr.next();
        if (key.endsWith(VALUELIST_SUFFIX)) {
          if (pmd.collectionType != PropertyMetadata.MULTIVALUED_COL_TYPE.LIST)
            log.warn("Expecting pmd's collectionType=LIST but got {} for prop={}", pmd.collectionType, key);
          if (pmd.addStrat == null)
            log.warn("No addStrat set; defaulting to APPEND: {}", pmd);
          if (pmd.addStrat == PropertyMetadata.MULTIVALUED_STRATEGY.PREPEND) {
            if (valueList instanceof List)
              ((List<Object>) valueList).add(0, fVal);
            else {
              log.warn("Expecting valueList to be a List but got {}", valueList.getClass());
              valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
            }
          } else {
            log.info("Appending to {} with {}", valueList, fVal);
            valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
          }
          toValueChanged = true;
        } else { // treat as a Set 
          if (pmd.collectionType != PropertyMetadata.MULTIVALUED_COL_TYPE.SET)
            log.warn("Expecting pmd's collectionType=SET but got {} for prop={}", pmd.collectionType, key);
          if (!valueList.contains(fVal)) { // not efficient since searching in a list
            valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
            toValueChanged = true;
          }
        }
      }
    } else { // fromValue is a single value
      if (key.endsWith(VALUELIST_SUFFIX)) {
        valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        toValueChanged = true;
      } else { // treat as a Set 
        if (!valueList.contains(fromValue)) {
          valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        }
      }
    }

    if (toValueChanged)
      toE.setProperty(key, tryConvertCollectionToArray(valueList));
  }
}
