function DimensionTreeMapModel() {
  this.metricId;
  this.metricName;
  this.heatmapFilters;
  this.currentStart;
  this.currentEnd;
  this.baselineStart;
  this.baselineEnd;

  this.heatmapData;

  this.heatmapMode = 'percentChange';
  this.compareMode = constants.DEFAULT_COMPARE_MODE;

  this.currentTotal = 0;
  this.baselineTotal = 0;
  this.absoluteChange = 0;
  this.percentChange = 0;
  this.dimensions = [];
  this.treeMapData = [];
}

DimensionTreeMapModel.prototype = {
  init: function (params) {
    if (params) {
      this.metricId = params.metricId;
      this.metricName = params.metricName;
      this.currentStart = params.heatMapCurrentStart || this.currentStart;
      this.currentEnd = params.heatMapCurrentEnd || this.currentEnd;
      this.baselineStart = params.heatMapBaselineStart || this.baselineStart;
      this.baselineEnd = params.heatMapBaselineEnd || this.baselineEnd;
      this.granularity = params.granularity || this.granularity;
      this.heatmapMode = params.heatmapMode || this.heatmapMode;
      this.heatmapFilters = Object.assign({}, params.heatmapFilters);
      this.compareMode = params.compareMode || this.compareMode;
    }
  },

  update() {
    if (this.metricId) {
      return dataService.fetchHeatmapData(
        this.metricId,
        this.currentStart,
        this.currentEnd,
        this.baselineStart,
        this.baselineEnd,
        this.heatmapFilters
      ).then((heatMapData) => {
        this.heatmapData = heatMapData;
        this.transformResponseData(heatMapData);
        return heatMapData;
      });
    }
  },

  transformResponseData: function (heatMapData) {
    if (heatMapData) {
      if (heatMapData.dimensions) {
        this.dimensions = heatMapData.dimensions;
        var treeMapData = [];
        for (var i in heatMapData.dimensions) {
          var dimension = heatMapData.dimensions[i];
          var dataKey = this.metricName + "." + dimension;
          var row = {"t": "0", "children": []};
          if (heatMapData.data && heatMapData.data[dataKey]) {
            var dimensionValueIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['dimensionValue'];
            var percentageChangeIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['percentageChange'];
            var currentValueIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['currentValue'];
            var contributionToOverallChangeIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['contributionToOverallChange'];
            var contributionChangeIndex = heatMapData.data[dataKey].schema.columnsToIndexMapping['contributionDifference'];
            for (var j in heatMapData.data[dataKey].responseData) {
              var record = heatMapData.data[dataKey].responseData[j];
              var item = {
                "t": record[dimensionValueIndex],
                "value": record[currentValueIndex],
                "percentageChange": record[percentageChangeIndex],
                "contributionChange": record[contributionChangeIndex],
                "contributionToOverallChange": record[contributionToOverallChangeIndex]
              };
              row.children.push(item);
            }
          }
          treeMapData.push(row);
        }
        this.treeMapData = treeMapData;
      }
      this.currentTotal = heatMapData.summary.simpleFields.currentTotal;
      this.baselineTotal = heatMapData.summary.simpleFields.baselineTotal;
      this.percentChange = heatMapData.summary.simpleFields.deltaPercentage;
      this.absoluteChange = heatMapData.summary.simpleFields.deltaChange;
    }
  }
}

