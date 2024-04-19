package io.seon.scoring.interview.do_not_touch.event;

public enum EventType {

    // Emitted by the framework
    GUESTS_ARRIVED,
    GUESTS_DRINKS_DONE,
    GUESTS_LEFT,
    VIP_GUEST_ARRIVED,
    BARTENDER_GOES_ON_BRAKE,


    // Emitted by interviewee
    WRONG_ORDER_DETAILS,
    GUESTS_KICKED_OUT

}
