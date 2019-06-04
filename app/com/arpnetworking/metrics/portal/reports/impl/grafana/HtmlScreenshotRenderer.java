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

package com.arpnetworking.metrics.portal.reports.impl.grafana;

import com.arpnetworking.metrics.portal.reports.RenderedReport;
import com.arpnetworking.metrics.portal.reports.Renderer;
import com.arpnetworking.metrics.portal.reports.impl.chrome.DevToolsFactory;
import com.arpnetworking.metrics.portal.reports.impl.chrome.DevToolsService;
import com.google.inject.Inject;
import models.internal.impl.DefaultRenderedReport;
import models.internal.impl.GrafanaReportPanelReportSource;
import models.internal.impl.HtmlReportFormat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Uses a headless Chrome instance to render a Grafana report panel as HTML.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class HtmlScreenshotRenderer implements Renderer<GrafanaReportPanelReportSource, HtmlReportFormat> {
    @Override
    public CompletionStage<RenderedReport> render(
            final GrafanaReportPanelReportSource source,
            final HtmlReportFormat format,
            final Instant scheduled
    ) {
        final DevToolsService dts = _devToolsFactory.create(source.getWebPageReportSource().ignoresCertificateErrors());
        final CompletableFuture<RenderedReport> result = new CompletableFuture<>();
        dts.onEvent("reportrendered", () -> {
            final String srcdoc = (String) dts.evaluate("document.getElementsByClassName('rendered-markdown-container')[0].srcdoc");
            result.complete(new DefaultRenderedReport.Builder()
                    .setFormat(format)
                    .setScheduledFor(scheduled)
                    .setGeneratedAt(Instant.now())
                    .setBytes(srcdoc.getBytes(StandardCharsets.UTF_8))
                    .build()
            );
        });
        dts.navigate(source.getWebPageReportSource().getUri().toString());
        return result;
    }

    /**
     * Public constructor.
     *
     * @param devToolsFactory the {@link DevToolsFactory} to use to create tabs.
     */
    @Inject
    protected HtmlScreenshotRenderer(final DevToolsFactory devToolsFactory) {
        _devToolsFactory = devToolsFactory;
    }

    private final DevToolsFactory _devToolsFactory;
}
