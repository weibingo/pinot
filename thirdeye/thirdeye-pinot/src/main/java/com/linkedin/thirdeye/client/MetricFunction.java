package com.linkedin.thirdeye.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.linkedin.thirdeye.constant.MetricAggFunction;

public class MetricFunction implements Comparable<MetricFunction> {

  private MetricAggFunction functionName;
  private String metricName;
  private Long metricId;
  private String dataset;

  public MetricFunction() {

  }

  public MetricFunction(@JsonProperty("functionName") MetricAggFunction functionName,
      @JsonProperty("metricName") String metricName, @JsonProperty("metricId") Long metricId,
      @JsonProperty("dataset") String dataset) {
    this.functionName = functionName;
    this.metricName = metricName;
    this.metricId = metricId;
    this.dataset = dataset;
  }

  private String format(String functionName, String metricName) {
    return String.format("%s_%s", functionName, metricName);
  }

  @Override
  public String toString() {
    // TODO this is hardcoded for pinot's return column name, but there's no binding contract that
    // clients need to return response objects with these keys.
   return format(functionName.name(), metricName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(functionName, metricName, metricId, dataset);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MetricFunction)) {
      return false;
    }
    MetricFunction mf = (MetricFunction) obj;
    return Objects.equal(functionName, mf.functionName)
        && Objects.equal(metricName, mf.metricName)
        && Objects.equal(metricId, mf.metricId)
        && Objects.equal(dataset, mf.dataset);
  }

  @Override
  public int compareTo(MetricFunction o) {
    return this.toString().compareTo(o.toString());
  }

  public MetricAggFunction getFunctionName() {
    return functionName;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public void setFunctionName(MetricAggFunction functionName) {
    this.functionName = functionName;
  }

  public Long getMetricId() {
    return metricId;
  }

  public void setMetricId(Long metricId) {
    this.metricId = metricId;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }


}
