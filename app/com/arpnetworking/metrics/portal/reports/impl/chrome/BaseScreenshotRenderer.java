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

import com.arpnetworking.metrics.portal.reports.RenderedReport;
import com.arpnetworking.metrics.portal.reports.Renderer;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.typesafe.config.Config;
import models.internal.TimeRange;
import models.internal.reports.ReportFormat;
import models.internal.reports.ReportSource;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Uses a headless Chrome instance to render a page as HTML.
 *
 * @param <S> the type of {@link ReportSource} to render
 * @param <F> the format to render into.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public abstract class BaseScreenshotRenderer<S extends ReportSource, F extends ReportFormat> implements Renderer<S, F> {

    /**
     * Get, for a given source, whether we can safely ignore a bad certificate.
     *
     * @param source the source we'll be rendering
     * @return whether we can safely ignore a bad certificate
     */
    protected abstract boolean getIgnoreCertificateErrors(S source);

    /**
     * Get the URI to visit in order to render a given source.
     *
     * @param source the source we'll be rendering
     * @return the URI to visit
     */
    protected abstract URI getURI(S source);

    /**
     * Called when the page we want to render has finished loading, i.e. the JavaScript {@code load} event has fired.
     *
     * @param result a {@link CompletableFuture} to complete when the page has been rendered
     * @param devToolsService a
     * @param source the source being rendered
     * @param format the format being rendered into
     * @param timeRange the time range being reported on
     * @param builder the {@link RenderedReport.Builder} to populate from the page
     * @param <B> the specific type of builder
     */
    protected abstract <B extends RenderedReport.Builder<B, ?>> void onLoad(
            CompletableFuture<B> result,
            DevToolsService devToolsService,
            S source,
            F format,
            TimeRange timeRange,
            B builder
    );

    @Override
    public <B extends RenderedReport.Builder<B, ?>> CompletionStage<B> render(
            final S source,
            final F format,
            final TimeRange timeRange,
            final B builder
    ) {
        final DevToolsService dts = _devToolsFactory.create(getIgnoreCertificateErrors(source), _chromeArgs);
        final CompletableFuture<B> result = new CompletableFuture<>();
        dts.onLoad(() -> onLoad(result, dts, source, format, timeRange, builder));
        dts.navigate(getURI(source).toString());
        result.whenComplete((x, e) -> dts.close());
        return result;
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
    protected BaseScreenshotRenderer(@Assisted final Config config) {
        _devToolsFactory = new DevToolsFactory(config.getString("chromePath"));
        _chromeArgs = config.getObject("chromeArgs").unwrapped();
    }

    private final DevToolsFactory _devToolsFactory;
    private final Map<String, Object> _chromeArgs;
}
