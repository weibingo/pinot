package com.linkedin.pinot.common.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractTableConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableConfig.class);

  private final String tableName;
  private final String tableType;
  private final SegmentsValidationAndRetentionConfig validationConfig;
  private final TenantConfig tenantConfig;
  private final TableCustomConfig customConfigs;

  private final Map<String, String> rawMap;

  protected AbstractTableConfig(String tableName, String tableType,
      SegmentsValidationAndRetentionConfig validationConfig, TenantConfig tenantConfig,
      TableCustomConfig customConfigs, Map<String, String> rawMap) {
    this.tableName = tableName;
    this.tableType = tableType;
    this.validationConfig = validationConfig;
    this.tenantConfig = tenantConfig;
    this.customConfigs = customConfigs;
    this.rawMap = rawMap;
  }

  public static AbstractTableConfig init(String jsonString) throws JSONException, JsonParseException,
      JsonMappingException, JsonProcessingException, IOException {
    JSONObject o = new JSONObject(jsonString);
    String tableName = o.getString("tableName");
    String tableType = o.getString("tableType").toLowerCase();
    SegmentsValidationAndRetentionConfig validationConfig =
        loadSegmentsConfig(new ObjectMapper().readTree(o.getJSONObject("segmentsConfig").toString()));
    TenantConfig tenantConfig = loadTenantsConfig(new ObjectMapper().readTree(o.getJSONObject("tenants").toString()));
    TableCustomConfig customConfig =
        loadCustomConfig(new ObjectMapper().readTree(o.getJSONObject("metadata").toString()));
    IndexingConfig config =
        loadIndexingConfig(new ObjectMapper().readTree(o.getJSONObject("tableIndexConfig").toString()));

    Map<String, String> rawMap = new HashMap<String, String>();
    rawMap.put("tableName", tableName);
    rawMap.put("tableType", tableType);
    rawMap.put("segmentsConfig", new ObjectMapper().writeValueAsString(validationConfig));
    rawMap.put("tenants", new ObjectMapper().writeValueAsString(tenantConfig));
    rawMap.put("metadata", new ObjectMapper().writeValueAsString(customConfig));
    rawMap.put("tableIndexConfig", new ObjectMapper().writeValueAsString(config));

    if (tableType.equals("offline")) {
      return new RealtimeTableConfig(tableName, tableType, validationConfig, tenantConfig, customConfig, rawMap, config);
    } else if (tableType.equals("realtime")) {
      return new OfflineTableConfig(tableName, tableType, validationConfig, tenantConfig, customConfig, rawMap, config);
    }
    throw new UnsupportedOperationException("unknown tableType : " + tableType);
  }

  public static AbstractTableConfig fromZnRecord(ZNRecord record) throws JsonParseException, JsonMappingException,
      JsonProcessingException, JSONException, IOException {
    Map<String, String> simpleFields = record.getSimpleFields();
    JSONObject str = new JSONObject();
    str.put("tableName", simpleFields.get("tableName"));
    str.put("tableType", simpleFields.get("tableType"));
    str.put("segmentsConfig", new JSONObject(simpleFields.get("segmentsConfig")));
    str.put("tenants", new JSONObject(simpleFields.get("tenants")));
    str.put("tableIndexConfig", new JSONObject(simpleFields.get("tableIndexConfig")));
    str.put("metadata", new JSONObject(simpleFields.get("metadata")));
    return init(str.toString());
  }

  public static ZNRecord toZnRecord(AbstractTableConfig config) throws JsonGenerationException, JsonMappingException,
      IOException {
    ZNRecord rec = new ZNRecord(config.getTableName() + "_" + config.getTableType());
    Map<String, String> map = config.getRawJSONMap();
    rec.setSimpleFields(map);
    return rec;
  }

  public static SegmentsValidationAndRetentionConfig loadSegmentsConfig(JsonNode node) throws JsonParseException,
      JsonMappingException, IOException {
    return new ObjectMapper().readValue(node, SegmentsValidationAndRetentionConfig.class);
  }

  public static TenantConfig loadTenantsConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, TenantConfig.class);
  }

  public static TableCustomConfig loadCustomConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, TableCustomConfig.class);
  }

  public static IndexingConfig loadIndexingConfig(JsonNode node) throws JsonParseException, JsonMappingException,
      IOException {
    return new ObjectMapper().readValue(node, IndexingConfig.class);
  }

  public Map<String, String> getRawJSONMap() throws JsonGenerationException, JsonMappingException, IOException {
    return rawMap;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableType() {
    return tableType;
  }

  public SegmentsValidationAndRetentionConfig getValidationConfig() {
    return validationConfig;
  }

  public TenantConfig getTenantConfig() {
    return tenantConfig;
  }

  public TableCustomConfig getCustomConfigs() {
    return customConfigs;
  }

  public abstract IndexingConfig getIndexingConfig();

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    result.append("tableName:" + tableName + "\n");
    result.append("tableType:" + tableType + "\n");
    result.append("tenant : " + tenantConfig.toString() + " \n");
    result.append("segments : " + validationConfig.toString() + "\n");
    result.append("customConfigs : " + customConfigs.toString() + "\n");
    return result.toString();
  }
}
