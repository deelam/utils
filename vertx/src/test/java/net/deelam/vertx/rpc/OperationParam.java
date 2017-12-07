package net.deelam.vertx.rpc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * OperationParam
 */

public class OperationParam  implements Serializable {
  @JsonProperty("key")
  private String key = null;

  @JsonProperty("description")
  private String description = null;

  /**
   * map of type for each parameter
   */
  public enum ValuetypeEnum {
    STRING("string"),
    
    URI("uri"),
    
    INT("int"),
    
    LONG("long"),
    
    FLOAT("float"),
    
    DOUBLE("double"),
    
    BOOLEAN("boolean"),
    
    ENUM("enum"),
    
    OPERATIONID("operationId");

    private String value;

    ValuetypeEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static ValuetypeEnum fromValue(String text) {
      for (ValuetypeEnum b : ValuetypeEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("valuetype")
  private ValuetypeEnum valuetype = null;

  @JsonProperty("required")
  private Boolean required = false;

  @JsonProperty("isMultivalued")
  private Boolean isMultivalued = false;

  @JsonProperty("defaultValue")
  private Object defaultValue = null;

  @JsonProperty("possibleValues")
  private List<Object> possibleValues = new ArrayList<Object>();

  public OperationParam key(String key) {
    this.key = key;
    return this;
  }

   /**
   * Get key
   * @return key
  **/
  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public OperationParam description(String description) {
    this.description = description;
    return this;
  }

   /**
   * Get description
   * @return description
  **/
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public OperationParam valuetype(ValuetypeEnum valuetype) {
    this.valuetype = valuetype;
    return this;
  }

   /**
   * map of type for each parameter
   * @return valuetype
  **/
  public ValuetypeEnum getValuetype() {
    return valuetype;
  }

  public void setValuetype(ValuetypeEnum valuetype) {
    this.valuetype = valuetype;
  }

  public OperationParam required(Boolean required) {
    this.required = required;
    return this;
  }

   /**
   * whether this parameter is required
   * @return required
  **/
  public Boolean getRequired() {
    return required;
  }

  public void setRequired(Boolean required) {
    this.required = required;
  }

  public OperationParam isMultivalued(Boolean isMultivalued) {
    this.isMultivalued = isMultivalued;
    return this;
  }

   /**
   * if true, then a list of values is expected
   * @return isMultivalued
  **/
  public Boolean getIsMultivalued() {
    return isMultivalued;
  }

  public void setIsMultivalued(Boolean isMultivalued) {
    this.isMultivalued = isMultivalued;
  }

  public OperationParam defaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
    return this;
  }

   /**
   * Get defaultValue
   * @return defaultValue
  **/
  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
  }

  public OperationParam possibleValues(List<Object> possibleValues) {
    this.possibleValues = possibleValues;
    return this;
  }

  public OperationParam addPossibleValuesItem(Object possibleValuesItem) {
    this.possibleValues.add(possibleValuesItem);
    return this;
  }

   /**
   * possible values from which to choose (if applicable)
   * @return possibleValues
  **/
  public List<Object> getPossibleValues() {
    return possibleValues;
  }

  public void setPossibleValues(List<Object> possibleValues) {
    this.possibleValues = possibleValues;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OperationParam operationParam = (OperationParam) o;
    return Objects.equals(this.key, operationParam.key) &&
        Objects.equals(this.description, operationParam.description) &&
        Objects.equals(this.valuetype, operationParam.valuetype) &&
        Objects.equals(this.required, operationParam.required) &&
        Objects.equals(this.isMultivalued, operationParam.isMultivalued) &&
        Objects.equals(this.defaultValue, operationParam.defaultValue) &&
        Objects.equals(this.possibleValues, operationParam.possibleValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, description, valuetype, required, isMultivalued, defaultValue, possibleValues);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class OperationParam {\n");
    
    sb.append("    key: ").append(toIndentedString(key)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    valuetype: ").append(toIndentedString(valuetype)).append("\n");
    sb.append("    required: ").append(toIndentedString(required)).append("\n");
    sb.append("    isMultivalued: ").append(toIndentedString(isMultivalued)).append("\n");
    sb.append("    defaultValue: ").append(toIndentedString(defaultValue)).append("\n");
    sb.append("    possibleValues: ").append(toIndentedString(possibleValues)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

