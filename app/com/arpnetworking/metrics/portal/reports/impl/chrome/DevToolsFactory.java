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

import com.github.kklisura.cdt.services.ChromeService;
import com.github.kklisura.cdt.services.types.ChromeTab;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * A factory that sits atop a Chrome instance and creates tabs / dev-tools instances.
 *
 * @author Spencer Pearson
 */
public final class DevToolsFactory {

    /**
     * Create a {@link DevToolsService}.
     *
     * @param ignoreCertificateErrors whether the created tab should ignore certificate errors when loading resources.
     * @return the created service.
     */
    public DevToolsService create(final boolean ignoreCertificateErrors) {
        final ChromeTab tab = _chromeServiceProvider.get().createTab();
        final com.github.kklisura.cdt.services.ChromeDevToolsService result = _chromeServiceProvider.get().createDevToolsService(tab);
        if (ignoreCertificateErrors) {
            result.getSecurity().setIgnoreCertificateErrors(true);
        }
        return new DevToolsServiceWrapper(result);
    }

    /**
     * Public constructor.
     *
     * @param chromeServiceProvider provides the Chrome instance that tabs are created for rendering on.
     *   Using {@link Provider} enables lazy instantiation, so nodes don't eagerly instantiate {@link ChromeService}.
     */
    @Inject
    public DevToolsFactory(final Provider<ChromeService> chromeServiceProvider) {
        _chromeServiceProvider = chromeServiceProvider;
    }

    private final Provider<ChromeService>  _chromeServiceProvider;
}
