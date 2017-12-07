package net.deelam.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * store primitives and HashSet
 */
@RequiredArgsConstructor
@Slf4j
public class JavaSetPropertyMerger implements PropertyMerger {

  @SuppressWarnings("unchecked")
  public void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if(key.length()==0)
        throw new IllegalArgumentException("Property key cannot be empty: "+fromE);
      
      if (key.equals(IdGraph.ID)) // needed, in case this method is called for GraphElements
        continue;

      Object fromValue = fromE.getProperty(key);
      if (fromValue == null) {
        continue;
      }

      // fromValue is not null at this point
      Object toValue = toE.getProperty(key);
      if (toValue == null) { // toNode does not have value
        if (isMultivalued(fromValue)) {
          toE.setProperty(key, new LinkedHashSet<>((Set<Object>) fromValue)); // don't need to clone() since values are primitives
        }else{
          toE.setProperty(key, fromValue);
        }
        continue;
      } else if (!isMultivalued(fromValue) && toValue.equals(fromValue)) {
        // nothing to do; values are the same
        continue;
      } else try {
        mergeValues(fromValue, toE, key);
      } catch (Exception e) {
        log.warn("Could not merge property values for key=" + key, e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void mergeValues(Object fromValue, Element toE, String key) {
    // toValue and fromValue are not null and not equal
      /* Possible cases:
       * fromValue=val toValue=val     ==> toValue=SET_VALUE  toValue__SET=Set<?>
       * fromValue=Set<?> toValue=val  ==> toValue=SET_VALUE  toValue__SET=Set<?>
       * fromValue=val toValue=Set<?>  ==>                    toValue__SET=Set<?>
       * fromValue=Set<?> toValue=Set<?>  ==>                 toValue__SET=Set<?>
       */

      // check special SET_SUFFIX property and create a Set if needed
      Object value = toE.getProperty(key);
      Set<Object> valueSet = (isMultivalued(value)) ? (Set<Object>) value : null;
      if (valueSet == null) {
        Object existingVal = toE.getProperty(key);
        valueSet = new LinkedHashSet<>();
        toE.setProperty(key, valueSet);
        valueSet.add(existingVal);
        //toE.setProperty(key, SET_VALUE);
        //setPropertyValueClass(toE, key, existingVal);
      }
      if (isMultivalued(fromValue)) { // then 
        valueSet.addAll((Set<Object>) fromValue);
      } else {
        valueSet.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
      }
  }
  
  @Override
  public boolean addProperty(Element elem, String key, Object value) {
    if(isMultivalued(value))
      throw new IllegalArgumentException("TODO: allow adding multivalued value?");
    Object existingVal = elem.getProperty(key);
    if(existingVal==null){
      elem.setProperty(key, value);
      return true;
    }else{
      mergeValues(existingVal, elem, key);
      return false;
    }
  }

  @Override
  public boolean isMultivalued(Object value) {
    if(value==null)
      return false;
    if(value instanceof Set)
      return true;
    return !isAllowedValue(value);
  }
  
  boolean isAllowedValue(Object value){
    return (value.getClass().isPrimitive() || validPropertyClasses.contains(value.getClass()));
  }
  
  @Getter
  private final Set<Class<?>> validPropertyClasses = new HashSet<>();
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
  
/*  static final String VALUE_CLASS_SUFFIX = "__jsonClass";

  private void setPropertyValueClass(Element elem, String key, Object value) {
    final String compClassPropKey = key + VALUE_CLASS_SUFFIX;
    elem.setProperty(compClassPropKey, value.getClass().getCanonicalName());
  }
*/

  @SuppressWarnings("unchecked")
  @Override
  public <T> List<T> getListProperty(Element elem, String key) {
    Object value = elem.getProperty(key);
    Set<T> valueSet = (isMultivalued(value)) ? (Set<T>) value : null;
    if (valueSet == null) {
      T val = elem.getProperty(key);
      if(val==null)
        return null;
      else {
        List<T> arr = new ArrayList<>();
        arr.add((T) val);
        return arr;
      }
    }else{
      return new ArrayList<T>(valueSet);
    }
  }
  
  @Override
  public int getListPropertySize(Element elem, String key){
    Set<Object> valueSet = elem.getProperty(key);
    if (valueSet == null) {
      Object val = elem.getProperty(key);
      if(val==null)
        return 0;
      else {
        return 1;
      }
    }else{
      return valueSet.size();
    }
  }

}
