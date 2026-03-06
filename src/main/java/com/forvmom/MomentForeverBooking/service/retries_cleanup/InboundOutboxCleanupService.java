package com.forvmom.MomentForeverBooking.service.retries_cleanup;

import com.forvmom.MomentForeverBooking.commons.EventConstants;
import com.forvmom.MomentForeverBooking.repository.InboundOutboxDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class InboundOutboxCleanupService {

    private static final Logger logger = LoggerFactory.getLogger(InboundOutboxCleanupService.class);
    private static final int HOURS_TO_KEEP = 24;

    @Autowired
    private InboundOutboxDao inboundOutboxDao;


    // Cleanup old PROCESSED records
    @Transactional
    public void cleanupPublishedRecords() {
        LocalDateTime cutoff = LocalDateTime.now().minusHours(HOURS_TO_KEEP);
        int deleted = inboundOutboxDao.deleteByStatusAndUpdatedAtBefore(EventConstants.PROCESSED, cutoff);
        if (deleted > 0) {
            logger.info("Cleaned up {} processed records older than {}", deleted, cutoff);
        }
    }
}