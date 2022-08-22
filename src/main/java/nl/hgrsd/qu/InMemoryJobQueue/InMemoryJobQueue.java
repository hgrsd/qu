package nl.hgrsd.qu.InMemoryJobQueue;

import nl.hgrsd.qu.JobQueue.Job;
import nl.hgrsd.qu.JobQueue.JobQueue;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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

    @Override
    public synchronized void scheduleJob(Job<T> job) {
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
    public Collection<Job<T>> pullJobs(Instant cutOff, int maxJobs) {
        return new ArrayList<>();
    }

    @Override
    public Optional<Job<T>> getJob(UUID id) {
        return Optional.ofNullable(indexById.get(id)).map(queue::get);
    }
}
