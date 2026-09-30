package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.JsonParser;
import io.github.green4j.jelly.JsonParserListenerAdapter;
import io.github.green4j.sdsm.DetailLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * What a client says, read into fields rather than into a tree:
 * <pre>
 * {"op":"subscribe","view":"topology"}
 * {"op":"unsubscribe","view":"topology"}
 * {"op":"interest","base":"COARSE","overrides":{"eu-de-1/blue/aggregator":"OFF"}}
 * </pre>
 * Reused: a frame is acted on before the next one is read, because both happen on the
 * session's own event loop, and what arrives there is valid only for the length of the call.
 */
final class InterestFrame extends JsonParserListenerAdapter {

    private static final String OVERRIDES = "overrides";

    private final JsonParser parser = new JsonParser();
    private final List<String> overrideIds = new ArrayList<>();
    private final List<DetailLevel> overrideLevels = new ArrayList<>();

    private String op;
    private String view;
    private DetailLevel base = DetailLevel.FINE;
    private String member;
    private int depth;
    private boolean unknownLevel;

    InterestFrame() {
        parser.setListener(this);
    }

    /**
     * @param frame what the client sent
     * @return whether it could be read, naming only levels there are, and says which op it is
     */
    boolean read(final CharSequence frame) {
        op = null;
        view = null;
        base = DetailLevel.FINE;
        member = null;
        depth = 0;
        unknownLevel = false;
        overrideIds.clear();
        overrideLevels.clear();
        parser.parseAndEoj(frame);
        return !parser.hasError() && op != null && !unknownLevel;
    }

    String op() {
        return op;
    }

    String view() {
        return view;
    }

    DetailLevel base() {
        return base;
    }

    int overrideCount() {
        return overrideIds.size();
    }

    String overrideIdAt(final int index) {
        return overrideIds.get(index);
    }

    DetailLevel overrideLevelAt(final int index) {
        return overrideLevels.get(index);
    }

    @Override
    public boolean onObjectStarted() {
        depth++;
        return true;
    }

    @Override
    public boolean onObjectEnded() {
        depth--;
        return true;
    }

    @Override
    public boolean onObjectMember(final CharSequence name) {
        member = name.toString();
        return true;
    }

    @Override
    public boolean onStringValue(final CharSequence data) {
        if (member == null) {
            return true;
        }
        if (depth > 1 && !OVERRIDES.equals(member)) {
            overrideIds.add(member);
            overrideLevels.add(levelOf(data));
            return true;
        }
        switch (member) {
            case "op":
                op = data.toString();
                break;
            case "view":
                view = data.toString();
                break;
            case "base":
                base = levelOf(data);
                break;
            default:
                break;
        }
        return true;
    }

    private DetailLevel levelOf(final CharSequence text) {
        final String name = text.toString().toUpperCase(Locale.ROOT);
        for (final DetailLevel level : DetailLevel.values()) {
            if (level.name().equals(name)) {
                return level;
            }
        }
        unknownLevel = true;
        return null;
    }
}
