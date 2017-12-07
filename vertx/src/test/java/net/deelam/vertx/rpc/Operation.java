package net.deelam.vertx.rpc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * describes available operations
 */

public class Operation  implements Serializable {
  @JsonProperty("id")
  private String id = null;

  @JsonProperty("description")
  private String description = null;

  @JsonProperty("level")
  private Integer level = null;

  @JsonProperty("info")
  private Map info = null;

  @JsonProperty("params")
  private List<OperationParam> params = new ArrayList<OperationParam>();

  @JsonProperty("subOperations")
  private OperationMap subOperations = null;

  public Operation id(String id) {
    this.id = id;
    return this;
  }

   /**
   * for use in Request objects
   * @return id
  **/
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Operation description(String description) {
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

  public Operation level(Integer level) {
    this.level = level;
    return this;
  }

   /**
   * abstraction level of this operation
   * minimum: 0.0
   * @return level
  **/
  public Integer getLevel() {
    return level;
  }

  public void setLevel(Integer level) {
    this.level = level;
  }

  public Operation info(Map info) {
    this.info = info;
    return this;
  }

   /**
   * Get info
   * @return info
  **/
  public Map getInfo() {
    return info;
  }

  public void setInfo(Map info) {
    this.info = info;
  }

  public Operation params(List<OperationParam> params) {
    this.params = params;
    return this;
  }

  public Operation addParamsItem(OperationParam paramsItem) {
    this.params.add(paramsItem);
    return this;
  }

   /**
   * ordered list of parameters
   * @return params
  **/
  public List<OperationParam> getParams() {
    return params;
  }

  public void setParams(List<OperationParam> params) {
    this.params = params;
  }

  public Operation subOperations(OperationMap subOperations) {
    this.subOperations = subOperations;
    return this;
  }

   /**
   * Get subOperations
   * @return subOperations
  **/
  public OperationMap getSubOperations() {
    return subOperations;
  }

  public void setSubOperations(OperationMap subOperations) {
    this.subOperations = subOperations;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Operation operation = (Operation) o;
    return Objects.equals(this.id, operation.id) &&
        Objects.equals(this.description, operation.description) &&
        Objects.equals(this.level, operation.level) &&
        Objects.equals(this.info, operation.info) &&
        Objects.equals(this.params, operation.params) &&
        Objects.equals(this.subOperations, operation.subOperations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, description, level, info, params, subOperations);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Operation {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    level: ").append(toIndentedString(level)).append("\n");
    sb.append("    info: ").append(toIndentedString(info)).append("\n");
    sb.append("    params: ").append(toIndentedString(params)).append("\n");
    sb.append("    subOperations: ").append(toIndentedString(subOperations)).append("\n");
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

