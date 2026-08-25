package org.openas2.processor.msgtracking;

import org.junit.jupiter.api.Test;
import org.openas2.message.AS2Message;
import org.openas2.message.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Every tracking event for a message is written to the same row keyed on the message ID, so the
 * events have to be persisted in the order they were generated. If a later event is written before
 * an earlier one the earlier one overwrites the state of the later one and the tracking record is
 * silently left showing a stale state (e.g. stuck on "msg_send_start" for a message that was sent
 * and acknowledged). Both writes succeed at the database level so nothing is logged.
 */
public class MsgTrackingEventOrderTest {

    /**
     * Records the order in which events reach persist() and holds up the first one so that a
     * tracking module which persists each event on its own thread will let the second event
     * overtake the first.
     */
    private static class OrderRecordingTrackingModule extends BaseMsgTrackingModule {

        private final List<String> persistedStates = Collections.synchronizedList(new ArrayList<String>());
        private final CountDownLatch releaseFirstEvent = new CountDownLatch(1);
        private final CountDownLatch secondEventReachedPersist = new CountDownLatch(1);

        @Override
        protected void persist(Message msg, Map<String, String> map) {
            String state = map.get(FIELDS.STATE);
            if (Message.MSG_STATE_SEND_START.equals(state)) {
                // Hold up the first event so a module that persists each event on its own thread
                // lets the second event past it
                try {
                    releaseFirstEvent.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                secondEventReachedPersist.countDown();
            }
            persistedStates.add(state);
        }

        public boolean isRunning() {
            return true;
        }

        public void start() {
        }

        public void stop() {
        }

        public boolean healthcheck(List<String> failures) {
            return true;
        }
    }

    @Test
    public void eventsArePersistedInTheOrderTheyWereGenerated() throws Exception {
        OrderRecordingTrackingModule module = new OrderRecordingTrackingModule();
        AS2Message msg = new AS2Message();
        msg.setMessageID("<event-order-test>");

        msg.setOption("STATE", Message.MSG_STATE_SEND_START);
        module.handle(TrackingModule.DO_TRACK_MSG, msg, msg.getOptions());

        msg.setOption("STATE", Message.MSG_STATE_MSG_SENT_MDN_RECEIVED_OK);
        module.handle(TrackingModule.DO_TRACK_MSG, msg, msg.getOptions());

        // Give the second event every chance to overtake the first one while it is held up. It must
        // not get through: it has to stay queued behind the event that was generated before it.
        module.secondEventReachedPersist.await(2, TimeUnit.SECONDS);

        // Let the first event complete then wait for the queued events to drain
        module.releaseFirstEvent.countDown();
        module.shutdownPersistExecutor();

        assertEquals(Arrays.asList(Message.MSG_STATE_SEND_START, Message.MSG_STATE_MSG_SENT_MDN_RECEIVED_OK),
                module.persistedStates,
                "tracking events must be persisted in the order they were generated otherwise an earlier"
                        + " state silently overwrites a later one");
    }
}
