package net.deelam.graph;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.boon.core.value.CharSequenceValue;
import org.boon.core.value.NumberValue;
import org.boon.core.value.ValueList;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * store primitives and JSON arrays
 */
@RequiredArgsConstructor
@Slf4j
public class JsonPropertyMerger implements PropertyMerger {

  @Override
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
      if (toValue == null) {
//        if (isMultivalued(fromValue)) {
//          if(fromValue instanceof Collection){ // created by JavaSetPropertyMerger
//            toE.setProperty(key, mapper.toJson(fromValue));
//          } else {
//            toE.setProperty(key, fromValue);
//          }
//        } else
          setElementProperty(toE, key, fromValue);
        continue;
      } else if (!key.endsWith(VALUELIST_SUFFIX) && toValue.equals(fromValue)) {
        // nothing to do; values are the same
        continue;
      } else try {
        mergeValues(fromValue, toE, key);
      } catch (Exception e) {
        log.warn("Could not merge property values for key=" + key, e);
      }
    } // for loop
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

  boolean isAllowedValue(Object value){
    return (value.getClass().isPrimitive() || validPropertyClasses.contains(value.getClass()));
  }

  // for Graphs (like Neo4j) that can only store primitives
  private void setElementProperty(Element elem, String key, Object value) {
    Class<?> valueClass=value.getClass();
    if (value.getClass().isArray()) {
      valueClass=value.getClass().getComponentType();
      if (Array.getLength(value) == 0) {
//        elem.setProperty(key, value);
        return;
      }
    }

    if (validPropertyClasses.contains(valueClass)) {
      elem.setProperty(key, value); // TODO: 6: for Titan, must use addProperty() to store as list or set if value is a Collection
    } else { // save as Json
      elem.setProperty(key, mapper.toJson(value));
    }
  }

  private ObjectMapper mapper = JsonFactory.create();
  private static final Set<String> allowedMultivaluedProps=new LinkedHashSet<>();
  public static void allowMultivaluedProperty(String propName){
    allowedMultivaluedProps.add(propName);
  }

  @SuppressWarnings("unchecked")
  public void mergeValues(Object fromValue, Element toE, String key) {
    // toValue and fromValue are not null and want to add fromValue to the list
    /* Possible cases:
     * fromValue=val toValue=val     ==> toValue__SET=Set<?>
     * fromValue=Set<?> toValue=val  ==> toValue__SET=Set<?>
     * fromValue=val toValue=Set<?>  ==> toValue__SET=Set<?>
     * fromValue=Set<?> toValue=Set<?>  ==> toValue__SET=Set<?>
     */

    // check special SET_SUFFIX property and create a Set if needed
    Object value = toE.getProperty(key);
    Object parsedV =value;
    if(value instanceof String)
      parsedV = parseValue((String) value);
    List<Object> valueList=null;
    if (!isAllowedValue(parsedV)) {
      valueList = (List<Object>) parsedV;
    }

    if (valueList == null) {
      valueList = new ValueList(false);
      valueList.add(value);
      if(!key.endsWith(VALUELIST_SUFFIX) && !allowedMultivaluedProps.contains(key)){
        log.warn("Property has multiple values which is inefficient: key="+key+" for node="+toE.getId()
            + " existingVal="+value+" addedValue="+fromValue/*, new Throwable("Call stack")*/);
      }
    }

    /// check if fromValue is already in toValueList
    boolean toValueChanged = false;
    if (isMultivalued(fromValue)) {
      String fromListStr = (String) fromValue; //fromE.getProperty(valSetPropKey);
      List<Object> fromValueList = (List<Object>) parseValue(fromListStr); //parseList(compClass, fromListStr);
      for (Object fVal : fromValueList) {
        if(key.endsWith(VALUELIST_SUFFIX)){
          valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        } else { // treat as a Set 
          if (!valueList.contains(fVal)) { // not efficient since searching in a list
//            if (!compClass.equals(fVal.getClass()))
//              log.warn("existingClass={} newValueClass={}", compClass, fVal.getClass());
            valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
            toValueChanged = true;
          }
        }
      }
    } else { // fromValue is a single value
      if(key.endsWith(VALUELIST_SUFFIX)){
        valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        toValueChanged = true;
      } else { // treat as a Set 
        if (!valueList.contains(fromValue)) {
//          if (!compClass.equals(fromValue.getClass()))
//            log.warn("existingClass={} newValueClass={}", compClass, fromValue.getClass());
          valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        }
      }
    }

    if (toValueChanged)
      toE.setProperty(key, mapper.toJson(valueList));
  }

  private Object parseValue(String jsonStr) {
    if(jsonStr==null)
      return null;
    try{
      if(jsonStr.length()==0)
        return jsonStr;
      Object parsedV = mapper.parser().parse(jsonStr);
      if(parsedV instanceof NumberValue)
        return ((NumberValue) parsedV).toValue();
      else if(parsedV instanceof CharSequenceValue)
        return jsonStr;
      else
        return parsedV;
    }catch(RuntimeException e){
      log.error("jsonStr="+jsonStr, e);
      throw new RuntimeException(e);
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
    if (value instanceof String) {
      Object parsedV = parseValue((String) value);
      return !isAllowedValue(parsedV);
    } else if (value instanceof Collection) {
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
    if(value==null)
      return null;
    else if (isMultivalued(value)){
      return (List<T>) parseValue((String) value);
    } else {
      List<T> arr = new ArrayList<>();
      arr.add((T) value);
      return arr;
    }
  }
  
  @Override
  public int getListPropertySize(Element elem, String key){
    final Object value = elem.getProperty(key);
    if(value==null)
      return 0;
    else if (isMultivalued(value)){
      return ((List<?>) parseValue((String) value)).size();
    } else {
      return 1;
    }
  }
  
  // TODO: 7: set limit on size of Set
  // TODO: 7: add supernode detection and warning

  public Map<String, Object> convertToJson(Map<String, Object> props){
    Map<String, Object> newMap=new HashMap<>();
    for(Entry<String, Object> e:props.entrySet()){
      if (isMultivalued(e.getValue())) { 
        newMap.put(e.getKey(), mapper.toJson(e.getValue()));
      }else if (validPropertyClasses.contains(e.getValue().getClass())) {
        newMap.put(e.getKey(), e.getValue());
      } else {// save as Json
        newMap.put(e.getKey(), mapper.toJson(e.getValue()));
      }
    }
    return newMap;
  }

  public void convertFromJson(Map<String, Object> existingProps, GrafRecord tempGr) {
    for(Entry<String, Object> entry:existingProps.entrySet()){
      Object value = entry.getValue();
      if(isMultivalued(value)){
        try{
          if(value instanceof String)
            value=parseValue((String) value);
          Collection<?> col;
          if(entry.getKey().endsWith(VALUELIST_SUFFIX)){
            col=(List<?>) value;
          }else{
            col=new LinkedHashSet<>((List<?>) value); //mapper.parser().parse(valueStr));
          }
          tempGr.setProperty(entry.getKey(), col);
        }catch(Exception e){
          log.error("{} {} {}", entry.getValue(), entry.getValue().getClass(), value);
          throw e;
        }
      }else{
        tempGr.setProperty(entry.getKey(), value);
      }
    }
  }
  
}
