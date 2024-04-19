package io.seon.scoring.interview.do_not_touch.event;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;


@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Event {

    Integer tableId;
    EventType type;
    String payload;

    public static Event of(Integer tableId, EventType eventType) {
        return new Event(tableId, eventType, null);
    }

    public static Event of(Integer tableId, EventType eventType, String payload) {
        return new Event(tableId, eventType, payload);
    }
}
