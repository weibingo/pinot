package com.linkedin.thirdeye.anomaly.views;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;


public class AnomalyTimelinesView {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  List<TimeBucket> timeBuckets = new ArrayList<>();
  Map<String, String> summary = new HashMap<>();
  List<Double> currentValues = new ArrayList<>();
  List<Double> baselineValues = new ArrayList<>();

  public List<TimeBucket> getTimeBuckets() {
    return timeBuckets;
  }

  public void addTimeBuckets(TimeBucket timeBucket) {
    this.timeBuckets.add(timeBucket);
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void addSummary(String key, String value) {
    this.summary.put(key, value);
  }

  public List<Double> getCurrentValues() {
    return currentValues;
  }

  public void addCurrentValues(Double currentValue) {
    this.currentValues.add(currentValue);
  }

  public List<Double> getBaselineValues() {
    return baselineValues;
  }

  public void addBaselineValues(Double baselineValue) {
    this.baselineValues.add(baselineValue);
  }

  /**
   * Convert current instance into JSON String using ObjectMapper
   *
   * NOTE, as long as the getter and setter is implemented, the ObjectMapper constructs the JSON String via
   * the getter and setter
   * @return
   *    The JSON String of current instance
   * @throws JsonProcessingException
   */
  public String toJsonString() throws JsonProcessingException {
    String jsonString = OBJECT_MAPPER.writeValueAsString(this);

    return jsonString;
  }

  /**
   * Given the JSON String of an AnomalyTimelinesView, return an instance of AnomalyTimelinesView
   *
   * NOTE, as long as the getter and setter is implemented, the ObjectMapper constructs the JSON String via
   * the getter and setter
   * @param jsonString
   *    The JSON String of an instance
   * @return
   *    An instance based on the given JSON String
   * @throws IOException
   */
  public static AnomalyTimelinesView fromJsonString(String jsonString) throws IOException {
    AnomalyTimelinesView view = OBJECT_MAPPER.readValue(jsonString, AnomalyTimelinesView.class);
    return view;
  }
}
