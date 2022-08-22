package nl.hgrsd.qu.JobQueue;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

public interface JobQueue<T> {
    void scheduleJob(Job<T> job);
    void deleteJob(UUID id);
    Collection<Job<T>> pullJobs(Instant cutOff, int maxJobs);
    Optional<Job<T>> getJob(UUID id);
}