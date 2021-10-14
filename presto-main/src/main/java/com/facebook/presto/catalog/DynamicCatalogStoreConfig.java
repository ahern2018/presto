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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.LegacyConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.List;
/**
 * @author ahern
 * @date 2021/10/11 18:52
 * @since 1.0
 */
public class DynamicCatalogStoreConfig {

    private Integer intervalTime = 60;

    private Boolean autoReload = false;

    public Integer getIntervalTime() {
        return intervalTime;
    }

    @Config("catalog.interval-time")
    public DynamicCatalogStoreConfig setIntervalTime(Integer intervalTime) {
        this.intervalTime = intervalTime;
        return this;
    }

    public Boolean getAutoReload() {
        return autoReload;
    }

    @Config("catalog.auto-reload")
    public DynamicCatalogStoreConfig setAutoReload(Boolean autoReload) {
        this.autoReload = autoReload;
        return this;
    }
}
