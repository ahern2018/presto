package com.facebook.presto.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * @author ahern
 * @date 2021/10/11 18:52
 * @since 1.0
 */
public class CatalogInfo {

    /**
     * Catalog 名称
     */
    private final String catalogName;

    /**
     * 连接名称
     */
    private final String connectorName;

    /**
     * 表明
     */
    private final String tableName;

    /**
     * Schema 信息(JSON数据)
     */
    private final Object schemaInfo;

    /**
     * Catalog 配置信息
     */
    private final Map<String, String> properties;

    @JsonCreator
    public CatalogInfo(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaInfo") Object schemaInfo,
            @JsonProperty("properties")  Map<String, String> properties) {
        this.catalogName = catalogName;
        this.connectorName = connectorName;
        this.tableName = tableName;
        this.schemaInfo = schemaInfo;
        this.properties = properties;
    }

    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getConnectorName() {
        return connectorName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public Object getSchemaInfo() {
        return schemaInfo;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "CatalogInfo{" +
                "catalogName='" + catalogName + '\'' +
                ", connectorName='" + connectorName + '\'' +
                ", properties=" + properties +
                '}';
    }
}
