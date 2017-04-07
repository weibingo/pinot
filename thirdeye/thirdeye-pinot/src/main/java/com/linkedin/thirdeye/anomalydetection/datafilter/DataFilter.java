package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.Map;

public interface DataFilter {

  /**
   * Sets parameters for this filter.
   * @param props the properties for this filter.
   */
  void setParameters(Map<String, String> props);

  /**
   * Returns if the given data, which is a time series with the given metric name and dimension map, passes the filter.
   * @param metricTimeSeries the context for retrieving the time series.
   * @param dimensionMap the dimension map to retrieve the time series.
   * @return true if the time series passes this filter.
   */
  boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap);
}