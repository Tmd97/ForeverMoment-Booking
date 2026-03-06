package com.forvmom.MomentForeverBooking.service.inbound;

import com.forvmom.MomentForeverBooking.domain.entity.OutgoingOutboxRecord;
import com.forvmom.MomentForeverBooking.events.InboundEvent;

public interface InboundBookingEventProcessor {
    OutgoingOutboxRecord process(InboundEvent event);
    String getSupportedEventType();  // each strategy declares which event type it handles
}
