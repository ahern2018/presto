/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalog;

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.spi.ConnectorId;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.codehaus.plexus.util.StringUtils;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

/**
 * @author ahern
 * @date 2021/10/11 18:52
 * @since 1.0
 */

@Path(value = "/api/v1/catalog")
public class DynamicCatalogStore {
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final CatalogManager catalogManager;
    private final Announcer announcer;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config, Announcer announcer, CatalogManager catalogManager, DynamicCatalogStoreConfig dynamicCatalogStoreConfig) {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()),
                dynamicCatalogStoreConfig.getAutoReload(),
                dynamicCatalogStoreConfig.getIntervalTime(),
                announcer,
                catalogManager);
    }

    public DynamicCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs, Boolean autoReload, Integer intervalTime, Announcer announcer, CatalogManager catalogManager) {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.announcer = announcer;
        this.catalogManager = catalogManager;
        if (autoReload) {
            Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> {
                try {
                    reload();
                } catch (Exception e) {
                    log.error("reload catalog error", e);
                }
            }, 60, intervalTime, TimeUnit.SECONDS);
        }

    }

    /**
     * 新增catalog
     *
     * @param catalogInfo catalog信息
     * @return response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response add(CatalogInfo catalogInfo) throws IOException {
        addCatalog(catalogInfo);
        return Response.ok().build();
    }

    /**
     * 删除catalog
     *
     * @param catalogInfo catalogInfo catalog信息
     * @return response
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response del(CatalogInfo catalogInfo) {
        delCatalog(catalogInfo);
        return Response.ok().build();
    }

    /**
     * 删除Catalog信息
     *
     * @param catalogInfo catalog信息
     */
    private void delCatalog(CatalogInfo catalogInfo) {
        removeOldData(catalogInfo);
        // update ConnectorIds
        updateConnectorId(catalogInfo.getCatalogName(), OperateEnum.DEL);
        catalogsLoaded.set(true);
    }

    /**
     * 增加Catalog信息
     *
     * @param catalogInfo catalog信息
     */
    private void addCatalog(CatalogInfo catalogInfo) throws IOException {
        removeOldData(catalogInfo);
        writeSchemaToFile(catalogInfo);
        writeCatalogToFile(catalogInfo);
        loadCatalog(catalogInfo.getCatalogName(), catalogInfo.getProperties());
        updateConnectorId(catalogInfo.getCatalogName(), OperateEnum.ADD);
        catalogsLoaded.set(true);
    }

    private void removeOldData(CatalogInfo catalogInfo) {
        Set<String> connectorIds = getConnectorIds(getPrestoAnnouncement(announcer.getServiceAnnouncements()));
        if (connectorIds.contains(catalogInfo.getCatalogName())) {
            // del local file
            delCatalogFile(catalogInfo);
            // del Catalog cache
            connectorManager.dropConnection(catalogInfo.getCatalogName());
        }
    }

    /**
     * 删除Catalog文件
     *
     * @param catalogInfo catalog信息
     */
    private void delCatalogFile(CatalogInfo catalogInfo) {
        String filePath = catalogConfigurationDir + "/" + catalogInfo.getCatalogName() + ".properties";
        FileUtil.delFile(filePath);
    }

    /**
     * 写catalog文件
     *
     * @param catalogInfo catalog信息
     * @throws IOException IO异常
     */
    private void writeCatalogToFile(CatalogInfo catalogInfo) throws IOException {
        File file = new File(catalogConfigurationDir + "/" + catalogInfo.getCatalogName() + ".properties");
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        FileUtil.writeProperties(file.getPath(), catalogInfo.getProperties(), false);
    }

    /**
     * 持久化Schema信息
     *
     * @param catalogInfo catalog 信息
     * @throws IOException IO异常
     */
    private void writeSchemaToFile(CatalogInfo catalogInfo) throws IOException {
        String connectorName = catalogInfo.getConnectorName();
        String tableName = catalogInfo.getTableName();
        Object schemaInfo = catalogInfo.getTableInfo();
        if (!(StringUtils.isEmpty(connectorName) || StringUtils.isEmpty(tableName) || Objects.isNull(schemaInfo))) {
            String filePath = "etc/" + connectorName + "/" + tableName + ".json";
            File file = new File(filePath);
            if (!file.getParentFile().exists()) {
                boolean finished = file.getParentFile().mkdirs();
                if (!finished) {
                    log.error(">>>>>>>>创建目录{}失败<<<<<<<<", file.getPath());
                    return;
                }
            }
            FileUtil.writeFile(filePath, JsonUtil.toJson(schemaInfo), false);
        }
    }

    private void reload() throws Exception {
        log.info("reload catalog info .");
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                log.debug(Files.getNameWithoutExtension(file.getName()));
                loadCatalog(file);
            }
        }
        reloadConnectorIds();
    }

    private void loadCatalog(File file) throws Exception {
        String catalogName = Files.getNameWithoutExtension(file.getName());
        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());
        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties) {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }
        log.info("-- Loading catalog %s --", catalogName);
        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            } else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }
        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);
        connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private void updateConnectorId(String catalogName, OperateEnum operate) {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        // get existing connectorIds
        Set<String> connectorIds = getConnectorIds(announcement);
        ConnectorId connectorId = new ConnectorId(catalogName);
        switch (operate) {
            case ADD:
                connectorIds.add(connectorId.toString());
                break;
            case DEL:
                connectorIds.remove(connectorId.toString());
                break;
            default:
                break;
        }
        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));
        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private void reloadConnectorIds() {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        Set<String> connectorIds = new LinkedHashSet<>();
        // automatically build connectorIds if not configured
        List<Catalog> catalogs = catalogManager.getCatalogs();
        catalogs.stream().map(Catalog::getConnectorId).map(Object::toString).forEach(connectorIds::add);
        // build announcement with updated sources
        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));
        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static Set<String> getConnectorIds(ServiceAnnouncement announcement) {
        // get existing connectorIds
        String property = nullToEmpty(announcement.getProperties().get("connectorIds"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        return new LinkedHashSet<>(values);
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements) {
        for (ServiceAnnouncement announcement : announcements) {
            if (null != announcement.getType() && announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }
}
