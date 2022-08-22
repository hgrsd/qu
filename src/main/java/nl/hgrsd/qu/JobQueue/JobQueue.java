package nl.hgrsd.qu.JobQueue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobQueue<T> {
    void scheduleJob(Job<T> job);
    void deleteJob(UUID id);
    List<Job<T>> pullJobs(Instant cutOff, int maxJobs);
    void completeJob(UUID id);
    void markJobAsFailed(UUID id);
    Optional<Job<T>> getJob(UUID id);
}