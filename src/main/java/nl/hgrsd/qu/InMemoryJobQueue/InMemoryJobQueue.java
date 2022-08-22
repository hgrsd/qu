package nl.hgrsd.qu.InMemoryJobQueue;

import nl.hgrsd.qu.JobQueue.Job;
import nl.hgrsd.qu.JobQueue.JobQueue;
import nl.hgrsd.qu.JobQueue.JobStatus;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;


public class InMemoryJobQueue<T> implements JobQueue<T> {
    private final ConcurrentNavigableMap<BigInteger, Job<T>> queue = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<UUID, BigInteger> indexById = new ConcurrentHashMap<>();
    private BigInteger nextJob = BigInteger.ZERO;

    @Override
    public void scheduleJob(Job<T> job) {
        queue.put(nextJob, job);
        indexById.put(job.id(), nextJob);
        nextJob = nextJob.add(BigInteger.ONE);
    }

    @Override
    public void deleteJob(UUID id) {
        var jobId = indexById.get(id);
        if (jobId != null) {
            indexById.remove(id);
            queue.remove(jobId);
        }
    }

    @Override
    public synchronized List<Job<T>> pullJobs(Instant cutOff, int maxJobs) {
        List<Job<T>> result = new ArrayList<>();
        var filtered = queue.entrySet().stream().filter(job -> job.getValue().getStatus() == JobStatus.QUEUED && job.getValue().scheduledFor().isEmpty() || job.getValue().scheduledFor().get().isBefore(cutOff)).limit(maxJobs).toList();
        for (var entry : filtered) {
            var val = entry.getValue();
            val.setStatus(JobStatus.IN_PROGRESS);
            result.add(val);
        }
        return result;
    }

    @Override
    public void completeJob(UUID id) {

    }

    @Override
    public void markJobAsFailed(UUID id) {

    }

    @Override
    public Optional<Job<T>> getJob(UUID id) {
        return Optional.ofNullable(indexById.get(id)).map(queue::get);
    }
}
