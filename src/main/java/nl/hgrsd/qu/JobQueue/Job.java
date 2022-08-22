package nl.hgrsd.qu.JobQueue;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public record Job<T>(T data, UUID id, JobStatus status, Optional<Instant> scheduledFor) {
}
