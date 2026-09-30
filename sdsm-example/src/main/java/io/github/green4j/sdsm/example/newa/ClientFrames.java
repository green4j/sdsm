package io.github.green4j.sdsm.example.newa;

import io.github.green4j.newa.websocket.ClientSession;
import io.github.green4j.newa.websocket.Receiver;
import io.github.green4j.newa.websocket.WsApiObserver;
import io.github.green4j.newa.websocket.WsApiObserverFactory;
import io.github.green4j.sdsm.DetailLevel;
import io.github.green4j.sdsm.Interest;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureObject;
import io.netty.channel.ChannelId;

import java.util.ArrayList;
import java.util.List;

/**
 * Where what a client says is acted on. The frame is read on the session's own event loop,
 * because what arrives there is valid only for the length of the call; what it says then goes
 * where it belongs. Subscribing is newa's. Interest is the structure's - the client names
 * things by the id the watched world knows them by, and only the structure's thread can turn
 * those into objects, so the resolution and the setting are one task on it.
 */
public final class ClientFrames implements Receiver.Text, WsApiObserverFactory {


    private final Structure structure;
    private final ViewChannel channel;
    /** One per event loop: sessions on several loops read their frames at once. */
    private final ThreadLocal<InterestFrame> frames = ThreadLocal.withInitial(InterestFrame::new);

    /**
     * @param structure where interest goes
     * @param channel   where subscriptions go
     */
    public ClientFrames(final Structure structure, final ViewChannel channel) {
        this.structure = structure;
        this.channel = channel;
    }

    @Override
    public void text(final ClientSession session,
                     final CharSequence message,
                     final boolean last) {
        if (!last) {
            session.send(ErrorFrame.of(null, "a frame at a time, please"));
            return;
        }
        final InterestFrame frame = frames.get();
        if (!frame.read(message)) {
            session.send(ErrorFrame.of(null, "unreadable frame"));
            return;
        }
        switch (frame.op()) {
            case "subscribe":
                subscribe(session, frame.view());
                return;
            case "unsubscribe":
                channel.unsubscribe(session, frame.view());
                return;
            case "interest":
                wants(clientOf(session), frame);
                return;
            default:
                session.send(ErrorFrame.of(frame.view(), "no such op: " + frame.op()));
        }
    }

    /**
     * Says how much of what one client is looking at. Levels are per object, and what reaches
     * the source that pays for a value is exactly this.
     *
     * @param client who is looking
     * @param asked  what they said
     */
    void wants(final String client, final InterestFrame asked) {
        final DetailLevel base = asked.base();
        final int count = asked.overrideCount();
        final String[] ids = new String[count];
        final DetailLevel[] levels = new DetailLevel[count];
        for (int i = 0; i < count; i++) {
            ids[i] = asked.overrideIdAt(i);
            levels[i] = asked.overrideLevelAt(i);
        }
        structure.run(() -> {
            Interest interest = Interest.of(base);
            for (int i = 0; i < ids.length; i++) {
                final StructureObject object = structure.findByExternalId(ids[i]);
                if (object != null) {
                    interest = interest.at(object.id(), levels[i]);
                }
            }
            structure.setInterest(client, interest);
        });
    }

    /**
     * A client that has gone wants nothing any more: without this every closed window would go
     * on holding up the level of what it had been looking at.
     *
     * @return an observer that forgets the client's interest when its session closes
     */
    @Override
    public WsApiObserver newObserver() {
        return new WsApiObserver() {
            private String client;

            @Override
            public void onSessionOpened(final ClientSession session) {
                client = clientOf(session);
            }

            @Override
            public void onSessionClosed(final long durationNanos) {
                if (client != null) {
                    structure.clearInterest(client);
                }
            }
        };
    }

    private void subscribe(final ClientSession session, final String view) {
        final List<CharSequence> unknown = new ArrayList<>(1);
        if (view == null || channel.subscribeForKnown(session, List.of(view), unknown) == 0) {
            session.send(ErrorFrame.of(view, "no such view"));
        }
    }

    /**
     * What to call this client's interest. The session's own user data is newa's - the
     * subscription support keeps a session's subscriptions there - so the name comes off the
     * connection instead: two windows of one browser are two clients, as they should be.
     *
     * @param session the session
     * @return the client name
     */
    private static String clientOf(final ClientSession session) {
        return clientOf(session.channel().id());
    }

    /**
     * @param connection the connection's id
     * @return the client name: the long form of the id, as the short one is four random bytes
     *         two connections can share
     */
    static String clientOf(final ChannelId connection) {
        return "ws-" + connection.asLongText();
    }
}
