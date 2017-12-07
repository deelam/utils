package net.deelam.graph;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections.iterators.ArrayIterator;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * store primitives or arrays of primitives
 */
@RequiredArgsConstructor
@Slf4j
public class Neo4jPropertyMerger implements PropertyMerger {

  @Override
  public void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if (key.length() == 0)
        throw new IllegalArgumentException("Property key cannot be empty: " + fromE);

      if (key.equals(IdGraph.ID)) // needed, in case this method is called for GraphElements
        continue;

      Object fromValue = fromE.getProperty(key);
      if (fromValue == null) {
        continue;
      }

      if (propKeysToCopy != null && !propKeysToCopy.contains(key))
        continue;

      // fromValue is not null at this point
      Object toValue = toE.getProperty(key);
      if (toValue == null) {
        setElementProperty(toE, key, fromValue);
        continue;
      } else if (!isMultivalued(fromValue) && toValue.equals(fromValue)) {
        // nothing to do; values are the same
        continue;
      } else
        try {
          mergeValues(fromValue, toE, key);
        } catch (Exception e) {
          log.warn("Could not merge property values for key=" + key, e);
        }
    }
  }

  protected Object[] tryConvertCollectionToArray(final Collection<?> collection) {
    if (maxValueListSize < 0 && collection.size() > 100) {
      log.warn("Collection size is large: {}", collection.size());
    }
    Object[] array = null;
    final Iterator<?> objects = collection.iterator();
    for (int i = 0; objects.hasNext(); i++) {
      Object object = objects.next();
      if (object instanceof String && ((String) object).length() > 400) {
        log.warn("String value is very long: {}", object);
      }
      if (!validPropertyClasses.contains(object.getClass()))
        throw new IllegalArgumentException("Not valid: " + object.getClass() + " in " + collection);
      if (array == null)
        array = (Object[]) Array.newInstance(object.getClass(), collection.size());
      //assertEquals(object.getClass(), objArr.getClass().getComponentType());
      try {
        array[i] = object;
      } catch (ArrayStoreException e) {
        log.error("{}!={}", array[0].getClass(), object.getClass());
        throw e;
      }
    }
    return array;
  }

  @SuppressWarnings("unchecked")
  protected <T> Collection<T> tryConvertArrayToCollection(final Object value) {
    if (value instanceof Collection)
      return (Collection<T>) value;
    if (value.getClass().isArray()) {
      ArrayList<T> list = new ArrayList<T>();
      int arrlength = Array.getLength(value);
      for (int i = 0; i < arrlength; i++) {
        T object = (T) Array.get(value, i);
        list.add(object);
      }
      return list;
    } else {
      throw new IllegalArgumentException("Not an array: class=" + value.getClass() + " " + value);
    }
  }

  @Getter
  protected final Set<Class<?>> validPropertyClasses = new HashSet<>();

  {
    // this list is created from Neo4j's PropertyStore class
    validPropertyClasses.add(String.class);
    validPropertyClasses.add(Integer.class);
    validPropertyClasses.add(Boolean.class);
    validPropertyClasses.add(Float.class);
    validPropertyClasses.add(Long.class);
    validPropertyClasses.add(Double.class);
    validPropertyClasses.add(Byte.class);
    validPropertyClasses.add(Character.class);
    validPropertyClasses.add(Short.class);

    //Titan supported property types: http://s3.thinkaurelius.com/docs/titan/0.5.0/schema.html#_defining_property_keys
  }

  protected boolean isAllowedValue(Object value) {
    return (value.getClass().isPrimitive() || validPropertyClasses.contains(value.getClass()));
  }

  // for Graphs (like Neo4j) that can only store primitives or arrays of primitives
  protected void setElementProperty(Element elem, String key, Object value) {
    if (value instanceof Collection) { // do this before checking if it is an array
      value = tryConvertCollectionToArray((Collection<?>) value);
    }
    Class<?> valueClass = value.getClass();
    if (value.getClass().isArray()) {
      valueClass = value.getClass().getComponentType();
      if (Array.getLength(value) == 0) {
        //        elem.setProperty(key, value);
        return;
      }
    }

    if (validPropertyClasses.contains(valueClass)) {
      // value may be an array
      elem.setProperty(key, value);
    } else {
      throw new IllegalArgumentException("Not expecting class=" + value.getClass() + " value=" + value);
      // save as Json: elem.setProperty(key, mapper.toJson(value));
    }
  }

  protected static final Set<String> allowedMultivaluedProps = new LinkedHashSet<>();

  public static void allowMultivaluedProperty(String propName) {
    allowedMultivaluedProps.add(propName);
  }

  @Setter
  protected int maxValueListSize = -1;

  private void mergeValues(Object fromValue, Element toE, String key) {
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
    if (isMultivalued(value))
      valueList = tryConvertArrayToCollection(value);

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
          valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        } else { // treat as a Set 
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

  @Override
  public boolean addProperty(Element elem, String key, Object value) {
    if (isMultivalued(value))
      throw new IllegalArgumentException("TODO: allow adding multivalued value?");
    Object existingVal = elem.getProperty(key);
    if (existingVal == null) {
      elem.setProperty(key, value);
      return true;
    } else {
      mergeValues(value, elem, key);
      return false;
    }
  }

  @Override
  public boolean isMultivalued(Object value) {
    if (value == null)
      return false;
    if (value instanceof Collection) {
      return true;
    } else if (value.getClass().isArray()) {
      return true;
    } else if (isAllowedValue(value)) {
      return false;
    } else {
      throw new IllegalStateException("Wasn't expecting value of class=" + value.getClass() + " value=" + value);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> List<T> getListProperty(Element elem, String key) {
    final Object value = elem.getProperty(key);
    if (value == null)
      return null;
    else if (value instanceof List)
      return (List<T>) value;
    else if (value.getClass().isArray()) {
      return (List<T>) Arrays.asList((Object[]) value);
    } else {
      List<T> arr = new ArrayList<>();
      arr.add((T) value);
      return arr;
    }
  }

  @Override
  public int getListPropertySize(Element elem, String key) {
    final Object value = elem.getProperty(key);
    if (value == null)
      return 0;
    else if (value instanceof List)
      return ((List<?>) value).size();
    else if (value.getClass().isArray()) {
      return ((Object[]) value).length;
    } else {
      return 1;
    }
  }

  public Map<String, Object> convertToNeo4j(Map<String, Object> props) {
    Map<String, Object> newMap = new HashMap<>();
    for (Entry<String, Object> e : props.entrySet()) {
      if (isMultivalued(e.getValue())) {
        if (e.getValue() instanceof Collection)
          newMap.put(e.getKey(), tryConvertCollectionToArray((Collection<?>) e.getValue()));
        else if (e.getValue().getClass().isArray())
          newMap.put(e.getKey(), e.getValue());
        else
          throw new IllegalArgumentException("How to convert from " + e.getValue().getClass());
      } else if (validPropertyClasses.contains(e.getValue().getClass())) {
        newMap.put(e.getKey(), e.getValue());
      } else {
        throw new IllegalStateException(
            "Wasn't expecting value of class=" + e.getValue().getClass() + " value=" + e.getValue());
      }
    }
    return newMap;
  }

  public void convertFromNeo4j(Map<String, Object> existingProps, GrafRecord tempGr) {
    for (Entry<String, Object> entry : existingProps.entrySet()) {
      Object value = entry.getValue();
      if (isMultivalued(value)) {
        try {
          Collection<?> col;
          if (entry.getKey().endsWith(VALUELIST_SUFFIX)) {
            col = tryConvertArrayToCollection(value);
          } else {
            col = new LinkedHashSet<>(tryConvertArrayToCollection(value));
          }
          tempGr.setProperty(entry.getKey(), col);
        } catch (Exception e) {
          log.error("{} {} {}", entry.getValue(), entry.getValue().getClass(), value);
          throw e;
        }
      } else {
        tempGr.setProperty(entry.getKey(), value);
      }
    }
  }

  private Set<String> propKeysToCopy = null;

  public void setPropertiesToCopy(Set<String> propsToCopy) {
    propKeysToCopy = propsToCopy;
  }

}
