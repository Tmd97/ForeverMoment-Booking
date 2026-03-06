package com.forvmom.MomentForeverBooking.service.inbound;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class InboundEventProcessorRegistry {

    private final Map<String, InboundBookingEventProcessor> processorMap;

    public InboundEventProcessorRegistry(List<InboundBookingEventProcessor> processors) {
        processorMap = processors.stream()
                .collect(Collectors.toMap(
                        InboundBookingEventProcessor::getSupportedEventType,
                        Function.identity()
                ));
    }

    public InboundBookingEventProcessor getProcessor(String eventType) {
        InboundBookingEventProcessor processor = processorMap.get(eventType);
        if (processor == null) {
            throw new IllegalArgumentException("No processor found for event type: " + eventType);
        }
        return processor;
    }
}