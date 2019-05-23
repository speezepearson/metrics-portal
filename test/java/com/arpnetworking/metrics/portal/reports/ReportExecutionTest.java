package com.arpnetworking.metrics.portal.reports;

import com.arpnetworking.metrics.portal.scheduling.impl.OneOffSchedule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import models.internal.impl.ChromeScreenshotReportSource;
import models.internal.impl.DefaultRecipient;
import models.internal.impl.DefaultRenderedReport;
import models.internal.impl.DefaultReport;
import models.internal.impl.HtmlReportFormat;
import models.internal.impl.PdfReportFormat;
import models.internal.reports.Recipient;
import models.internal.reports.Report;
import models.internal.reports.ReportFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ReportExecutionTest {

    private static final Recipient ALICE = new DefaultRecipient.Builder()
            .setId(UUID.randomUUID())
            .setAddress("alice@invalid.com")
            .setType(RecipientType.EMAIL)
            .build();
    private static final Recipient BOB = new DefaultRecipient.Builder()
            .setId(UUID.randomUUID())
            .setAddress("bob@invalid.com")
            .setType(RecipientType.EMAIL)
            .build();

    private static final HtmlReportFormat HTML = new HtmlReportFormat.Builder().build();
    private static final PdfReportFormat PDF = new PdfReportFormat.Builder().setWidthInches(8.5f).setHeightInches(11f).build();
    private static final Instant T0 = Instant.now();
    private static final Report EXAMPLE_REPORT = new DefaultReport.Builder()
            .setId(UUID.randomUUID())
            .setName("My Name")
            .setReportSource(new ChromeScreenshotReportSource.Builder()
                    .setId(UUID.randomUUID())
                    .setTitle("My Report Title")
                    .setTriggeringEventName("myTriggeringEventName")
                    .setUri(URI.create("https://example.com"))
                    .setIgnoreCertificateErrors(true)
                    .build()
            )
            .setSchedule(new OneOffSchedule.Builder().setRunAtAndAfter(T0).build())
            .setRecipients(ImmutableSetMultimap.<ReportFormat, Recipient>builder()
                    .put(HTML, ALICE)
                    .put(HTML, BOB)
                    .put(PDF, BOB)
                    .build()
            )
            .build();


    private Injector _injector;
    @Mock
    private Sender _sender;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        Mockito.doReturn(CompletableFuture.completedFuture(null)).when(_sender).send(Mockito.any(), Mockito.any());
        _injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Sender.class).annotatedWith(Names.named("email")).toInstance(_sender);
                bind(Renderer.class).annotatedWith(Names.named("web html")).to(MockHtmlRenderer.class).asEagerSingleton();
                bind(Renderer.class).annotatedWith(Names.named("web pdf")).to(MockPdfRenderer.class).asEagerSingleton();
            }
        });
    }

    @Test
    public void testExecute() throws Exception {
        ReportExecution.execute(EXAMPLE_REPORT, _injector, T0).toCompletableFuture().get();

        Mockito.verify(_sender).send(ALICE, ImmutableMap.of(HTML, mockRendered(HTML, T0)));
        Mockito.verify(_sender).send(BOB, ImmutableMap.of(HTML, mockRendered(HTML, T0), PDF, mockRendered(PDF, T0)));
    }

    @Test(expected = ConfigurationException.class)
    public void testExecuteThrowsIfNoRendererFound() {
        ReportExecution.execute(EXAMPLE_REPORT, Guice.createInjector(), T0);
    }

    @Test(expected = ConfigurationException.class)
    public void testExecuteThrowsIfNoSenderFound() {
        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(Renderer.class).annotatedWith(Names.named("web html")).to(MockHtmlRenderer.class).asEagerSingleton();
                bind(Renderer.class).annotatedWith(Names.named("web pdf")).to(MockPdfRenderer.class).asEagerSingleton();
            }
        });
        ReportExecution.execute(EXAMPLE_REPORT, injector, T0);
    }

    private static DefaultRenderedReport mockRendered(final ReportFormat format, final Instant scheduled) {
        return new DefaultRenderedReport.Builder()
                .setBytes(new byte[0])
                .setFormat(format)
                .setScheduledFor(scheduled)
                .setGeneratedAt(scheduled)
                .build();
    }

    private static final class MockHtmlRenderer implements Renderer<ChromeScreenshotReportSource, HtmlReportFormat> {
        @Override
        public CompletionStage<RenderedReport> render(
                final ChromeScreenshotReportSource source,
                final HtmlReportFormat format,
                final Instant scheduled
        ) {
            return CompletableFuture.completedFuture(mockRendered(format, scheduled));
        }
    }

    private static final class MockPdfRenderer implements Renderer<ChromeScreenshotReportSource, PdfReportFormat> {
        @Override
        public CompletionStage<RenderedReport> render(
                final ChromeScreenshotReportSource source,
                final PdfReportFormat format,
                final Instant scheduled
        ) {
            return CompletableFuture.completedFuture(mockRendered(format, scheduled));
        }
    }
}