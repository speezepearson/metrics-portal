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

package com.arpnetworking.metrics.portal.reports.impl.chrome.grafana;

import com.arpnetworking.metrics.portal.reports.RenderedReport;
import com.arpnetworking.metrics.portal.reports.impl.chrome.DevToolsService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.typesafe.config.Config;
import models.internal.TimeRange;
import models.internal.impl.GrafanaReportPanelReportSource;
import models.internal.impl.HtmlReportFormat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * Uses a headless Chrome instance to render a page as HTML.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class HtmlScreenshotRenderer extends BaseScreenshotRenderer<HtmlReportFormat> {

    @Override
    protected <B extends RenderedReport.Builder<B, ?>> void onReportRendered(
            final CompletableFuture<B> result,
            final DevToolsService devToolsService,
            final GrafanaReportPanelReportSource source,
            final HtmlReportFormat format,
            final TimeRange timeRange,
            final B builder
    ) {
        final String html = (String) devToolsService.evaluate(
                "document.getElementsByClassName('rendered-markdown-container')[0].srcdoc"
        );
        result.complete(builder.setBytes(html.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Public constructor.
     *
     * @param config the configuration for this renderer. Meaningful keys:
     * <ul>
     *   <li>{@code chromePath} -- the path to the Chrome binary to use to render pages.</li>
     * </ul>
     */
    @Inject
    public HtmlScreenshotRenderer(@Assisted final Config config) {
        super(config);
    }
}