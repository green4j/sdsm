package io.github.green4j.sdsm.example.newa;

import io.github.green4j.newa.rest.HttpException;
import io.github.green4j.newa.rest.NamedMultiValues;
import io.github.green4j.newa.rest.RestApi;
import io.github.green4j.newa.rest.RestApiBuilder;
import io.github.green4j.newa.rest.RestContext;
import io.github.green4j.newa.rest.RestHandle;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;

/**
 * Views over HTTP, on the websocket's own port: what is on the air, a new one, and taking one
 * off. A view made here is subscribed to the same way as {@code topology} - by name, over the
 * socket.
 * <pre>
 * GET    /v1/views
 * POST   /v1/views          name, selector, [keys...], [intervalMs]   form-encoded
 * DELETE /v1/views/{name}
 * </pre>
 * Making and removing a view is the structure's to do, on its own thread, so both answer when
 * it has; the event loop never waits for it.
 */
public final class ViewsApi {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]{1,64}");
    private static final long DEFAULT_INTERVAL_MS = 100L;

    /**
     * @param version the api version, the prefix of every path
     * @param channel where views are published
     * @return the api
     */
    public static RestApi of(final int version, final ViewChannel channel) {
        final RestApiBuilder builder =
                new RestApiBuilder("SDSM views", "Views on the air", version, "1.0.0");

        builder.getJson("/views", (context, out) -> {
            out.startArray();
            channel.forEachSubscription(feed -> feed.describe(out));
            out.endArray();
        });

        builder.post("/views", (context, result) -> create(channel, context, result));

        builder.delete("/views/{name}", (context, result) -> {
            final String name = context.pathParameters().valueRequired("name");
            channel.withdraw(name).whenComplete((existed, failure) ->
                    context.executor().execute(() -> {
                        if (failure != null) {
                            result.error(asHttp(failure));
                        } else if (existed) {
                            result.respond(HttpResponseStatus.NO_CONTENT);
                        } else {
                            result.error(new HttpException(HttpResponseStatus.NOT_FOUND,
                                    "No view named '" + name + "'"));
                        }
                    }));
        }).withPathParameterDescriptions("name - the view to take off the air");

        return builder.build();
    }

    private static void create(final ViewChannel channel,
                               final RestContext context,
                               final RestHandle.Result result) throws HttpException {
        final NamedMultiValues form = context.formParameters();
        final String name = form.valueRequired("name");
        if (!NAME.matcher(name).matches()) {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST,
                    "A view name is 1 to 64 of letters, digits, '.', '_' and '-'");
        }
        final String selector = form.valueRequired("selector");
        final Duration interval =
                Duration.ofMillis(form.valueAsLong("intervalMs", DEFAULT_INTERVAL_MS));
        if (interval.isNegative()) {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST, "intervalMs is negative");
        }
        final Set<String> keys = keysOf(form);

        channel.publish(name, selector, keys, interval).whenComplete((feed, failure) ->
                context.executor().execute(() -> {
                    if (failure != null) {
                        result.error(asHttp(failure));
                        return;
                    }
                    final byte[] body = ("{\"view\":\"" + name + "\"}")
                            .getBytes(StandardCharsets.US_ASCII);
                    result.respond(HttpResponseStatus.CREATED, null,
                                    HttpHeaderValues.APPLICATION_JSON, body.length)
                            .append(body, 0, body.length)
                            .done();
                }));
    }

    private static Set<String> keysOf(final NamedMultiValues form) {
        final int count = form.numberOfValues("keys");
        if (count == 0) {
            return null;                // every property
        }
        final Set<String> keys = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            keys.add(form.value("keys", i));
        }
        return keys;
    }

    // What the structure refused is the client's to fix; a name already on the air is a
    // conflict; anything else is ours.
    private static Exception asHttp(final Throwable failure) {
        final Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
        if (cause instanceof IllegalStateException) {
            return new HttpException(HttpResponseStatus.CONFLICT, cause.getMessage());
        }
        if (cause instanceof IllegalArgumentException) {
            return new HttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage());
        }
        return cause instanceof Exception ? (Exception) cause : new Exception(cause);
    }

    private ViewsApi() {
    }
}
