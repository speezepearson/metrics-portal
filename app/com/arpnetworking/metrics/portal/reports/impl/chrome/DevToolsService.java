/*
 * Copyright 2018 Dropbox, Inc.
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

/**
 * A relatively minimal interface for a Chrome tab's dev tools.
 *
 * @author Spencer Pearson
 */
public interface DevToolsService {
    /**
     * Evaluates some JavaScript in the tab.
     *
     * @param js A JavaScript expression. (If you need multiple statements, wrap them in `(() => {...})()`.)
     * @return The result of the evaluation. (e.g. a String, a Double, a-- I don't know about arrays/objects.)
     */
    Object evaluate(String js);

    /**
     * Creates a PDF capturing how the page currently displays.
     *
     * @param pageWidth duh
     * @param pageHeight duh
     * @return Raw bytes of the PDF, suitable for e.g. writing to a .pdf file.
     */
    byte[] printToPdf(double pageWidth, double pageHeight);

    /**
     * Forces the tab to navigate to a new URL.
     *
     * @param url duh
     */
    void navigate(String url);

    /**
     * Registers a callback to get registered whenever a page loads.
     *
     * @param callback duh
     */
    void onLoad(Runnable callback);

    /**
     * Registers a callback to get called when a JavaScript event fires.
     *
     * @param eventName Name of the JavaScript event (e.g. "click").
     * @param callback The function to call when the event fires.
     */
    void onEvent(String eventName, Runnable callback);

    /**
     * Closes the dev tools. After close() is called, any further interaction is illegal.
     */
    void close();
}
