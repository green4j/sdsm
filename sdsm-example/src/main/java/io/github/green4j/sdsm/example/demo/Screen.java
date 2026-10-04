package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.ObjectKind;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Draws the shop out of the client view alone - what a browser at the other end of a socket would
 * have to draw with. A tier shows what the structure summed for it; a service shows whom it
 * calls, and a call nobody answers is shown as such.
 */
public final class Screen {

    private static final String RESET = "\u001b[0m";
    private static final String BOLD = "\u001b[1m";
    private static final String DIM = "\u001b[2m";
    private static final String RED = "\u001b[31m";
    private static final String GREEN = "\u001b[32m";
    private static final String YELLOW = "\u001b[33m";

    private static final Comparator<ClientObject> BY_NAME =
            Comparator.comparing(ClientObject::name);

    private final ClientView view;
    private final boolean colour;

    /**
     * @param view what to draw
     * @param colour whether to colour it
     */
    public Screen(final ClientView view, final boolean colour) {
        this.view = view;
        this.colour = colour;
    }

    /**
     * @param tick  the tick of the world
     * @param event what happened, or null
     * @return the frame
     */
    public String render(final long tick, final String event) {
        synchronized (view) {
            final StringBuilder frame = new StringBuilder(1024);
            final List<ClientObject> services = view.ofType(Shop.SERVICE);
            frame.append(paint(BOLD, "SDSM demo")).append("   tick ").append(tick)
                    .append("   services ").append(services.size())
                    .append("   links ").append(view.links().size())
                    .append("\n\n");
            final List<ClientObject> tiers = view.rootsOf(Shop.TIER);
            tiers.sort(BY_NAME);
            for (final ClientObject tier : tiers) {
                tier(frame, tier);
            }
            frame.append('\n').append(event == null ? "" : paint(YELLOW, event)).append('\n');
            return frame.toString();
        }
    }

    private void tier(final StringBuilder frame, final ClientObject tier) {
        frame.append(paint(BOLD, String.format("%-44s", tier.name())))
                .append(String.format("rps %5s", number(tier, "rps")));
        if (tier.has("down") && tier.longOf("down") > 0) {
            frame.append("   ").append(paint(RED, "down " + tier.longOf("down")));
        }
        frame.append('\n');
        final List<ClientObject> members = new ArrayList<>();
        for (final long id : tier.members()) {
            final ClientObject member = view.byId(id);
            if (member != null && member.kind() == ObjectKind.NODE) {
                members.add(member);
            }
        }
        members.sort(BY_NAME);
        for (final ClientObject service : members) {
            service(frame, service);
        }
    }

    private void service(final StringBuilder frame, final ClientObject service) {
        final boolean up = "UP".equals(service.textOf("status"));
        frame.append(String.format("  %-10s ", service.name()))
                .append(paint(up ? GREEN : RED, String.format("%-5s", service.textOf("status"))))
                .append(String.format(" %s/%s   ", number(service, "ready"),
                        number(service, "replicas")))
                .append(String.format("rps %5s", number(service, "rps")));
        final List<ClientObject> calls = new ArrayList<>();
        for (final ClientObject object : view.ofType("http")) {
            if (object.kind() == ObjectKind.OUTPUT && object.node() == service.id()) {
                calls.add(object);
            }
        }
        calls.sort(BY_NAME);
        String separator = "   -> ";
        for (final ClientObject call : calls) {
            frame.append(separator);
            separator = ", ";
            final boolean answered = call.has("$links") && call.longOf("$links") > 0;
            frame.append(answered ? call.name() : paint(DIM, call.name() + " (nobody)"));
        }
        frame.append('\n');
    }

    private String paint(final String how, final String text) {
        return colour ? how + text + RESET : text;
    }

    private static String number(final ClientObject object, final String key) {
        return object.has(key) ? Long.toString(object.longOf(key)) : "-";
    }
}
