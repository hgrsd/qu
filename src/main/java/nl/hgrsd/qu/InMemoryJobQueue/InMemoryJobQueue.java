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

public class InMemoryJobQueue<T> implements JobQueue<T> {
    private final ConcurrentNavigableMap<BigInteger, Job<T>> queue = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<UUID, BigInteger> indexById = new ConcurrentHashMap<>();
    private BigInteger nextJob = BigInteger.ZERO;

    private Optional<Job<T>> _getJobMutably(UUID id) {
        var jobId = indexById.get(id);
        if (jobId == null) return Optional.empty();
        var job = queue.get(jobId);
        return Optional.ofNullable(job);
    }

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
        var filtered = queue.values().stream()
                .filter(job -> job.getStatus() == JobStatus.QUEUED
                        && (job.scheduledFor().isEmpty()
                                || job.scheduledFor().get().isBefore(cutOff)))
                .limit(maxJobs)
                .toList();

        for (var job : filtered) {
            try {
                job.setStatus(JobStatus.IN_PROGRESS);
                result.add((Job<T>) job.clone());
            } catch (CloneNotSupportedException e) {
                job.setStatus(JobStatus.QUEUED);
                throw new RuntimeException(e);
            }
        }

        return result;
    }

    @Override
    public void completeJob(UUID id) {
        _getJobMutably(id).ifPresent(j -> j.setStatus(JobStatus.COMPLETED));
    }

    @Override
    public void markJobAsFailed(UUID id) {
        _getJobMutably(id).ifPresent(j -> j.setStatus(JobStatus.FAILED));
    }

    @Override
    public Optional<Job<T>> getJob(UUID id) {
        return Optional.ofNullable(indexById.get(id)).map(queue::get).map(job -> {
            try {
                return (Job<T>) job.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
