/*
 * Copyright 2019 Dropbox, Inc.
 *
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
package com.arpnetworking.metrics.portal.reports.impl.chrome;

import com.github.kklisura.cdt.launch.ChromeLauncher;
import com.github.kklisura.cdt.launch.config.ChromeLauncherConfiguration;
import com.github.kklisura.cdt.launch.support.impl.ProcessLauncherImpl;
import com.github.kklisura.cdt.services.ChromeService;
import com.github.kklisura.cdt.services.types.ChromeTab;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * A factory that sits atop a Chrome instance and creates tabs / dev-tools instances.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class DevToolsFactory {

    /**
     * Create a {@link DevToolsService}.
     *
     * @param ignoreCertificateErrors whether the created tab should ignore certificate errors when loading resources.
     * @return the created service.
     */
    public DevToolsService create(final boolean ignoreCertificateErrors) {
        final ChromeService service = CHROME_SERVICE_BY_PATH.computeIfAbsent(_chromePath, s -> createService());
        final ChromeTab tab = service.createTab();
        final com.github.kklisura.cdt.services.ChromeDevToolsService result = service.createDevToolsService(tab);
        if (ignoreCertificateErrors) {
            result.getSecurity().setIgnoreCertificateErrors(true);
        }
        return new DevToolsServiceWrapper(result);
    }

    /**
     * Public constructor.
     *
     * @param chromePath is the path to the Chrome binary to use.
     */
    public DevToolsFactory(final String chromePath) {
        _chromePath = chromePath;
    }

    private ChromeService createService() {
        // The config should be able to override the CHROME_PATH environment variable that ChromeLauncher uses.
        // This requires in our own custom "environment" (since it defaults to using System::getEnv).
        final ImmutableMap<String, String> env = ImmutableMap.of(
                ChromeLauncher.ENV_CHROME_PATH, _chromePath
        );
        // ^^^ In order to pass this environment in, we need to use a many-argument constructor,
        //   which doesn't have obvious default values. So I stole the arguments from the fewer-argument constructor:
        // CHECKSTYLE.OFF: LineLength
        //   https://github.com/kklisura/chrome-devtools-java-client/blob/master/cdt-java-client/src/main/java/com/github/kklisura/cdt/launch/ChromeLauncher.java#L105
        // CHECKSTYLE.ON: LineLength
        final ChromeLauncher launcher = new ChromeLauncher(
                new ProcessLauncherImpl(),
                env::get,
                new ChromeLauncher.RuntimeShutdownHookRegistry(),
                new ChromeLauncherConfiguration()
        );
        return launcher.launch(true);

    }

    private final String _chromePath;

    private static final Map<String, ChromeService> CHROME_SERVICE_BY_PATH = Maps.newHashMap();
}