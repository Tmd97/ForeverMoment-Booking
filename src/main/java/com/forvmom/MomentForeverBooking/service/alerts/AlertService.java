package com.forvmom.MomentForeverBooking.service.alerts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AlertService {
    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    public void sendAlert(String message) {
        // In a real system, integrate with Slack, PagerDuty, email, etc.
        log.error("🔔 ALERT: {}", message);
    }
}