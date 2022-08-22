package nl.hgrsd.qu.InMemoryJobQueue;

import nl.hgrsd.qu.JobQueue.Job;
import nl.hgrsd.qu.JobQueue.JobStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class InMemoryJobQueueTest {
    @Test
    public void InMemoryJobQueue_SchedulesJob() {
        var j = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j);

        var job = qu.getJob(j.id());
        Assertions.assertEquals(j, job.get());
    }

    @Test
    public void InMemoryJobQueue_ReturnsEmpty() {
        var qu = new InMemoryJobQueue<String>();
        Assertions.assertEquals(Optional.empty(), qu.getJob(UUID.randomUUID()));
    }

    @Test
    void InMemoryJobQueue_DeletesJob() {
        var j = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j);
        qu.deleteJob(j.id());

        Assertions.assertEquals(Optional.empty(), qu.getJob(j.id()));
    }

    @Test
    public void InMemoryJobQueue_ReturnsJobsInOrder() {
        var j0 = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var j1 = new Job<>("Test1", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var j2 = new Job<>("Test2", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j0);
        qu.scheduleJob(j1);
        qu.scheduleJob(j2);

        var jobs = qu.pullJobs(Instant.MAX, 10);

        Assertions.assertEquals(3, jobs.size());
        Assertions.assertEquals(new Job<>(j0.data(), j0.id(), JobStatus.IN_PROGRESS, j0.scheduledFor()), jobs.get(0));
        Assertions.assertEquals(new Job<>(j1.data(), j1.id(), JobStatus.IN_PROGRESS, j1.scheduledFor()), jobs.get(1));
        Assertions.assertEquals(new Job<>(j2.data(), j2.id(), JobStatus.IN_PROGRESS, j2.scheduledFor()), jobs.get(2));
    }

    @Test
    public void InMemoryJobQueue_ReturnsJobsUsingCutoff() {
        var j0 = new Job<>(
                "Should Return",
                UUID.randomUUID(),
                JobStatus.QUEUED,
                Optional.of(Instant.parse("2022-01-01T00:00:00.000Z")));
        var j1 = new Job<>(
                "Should Not Return",
                UUID.randomUUID(),
                JobStatus.QUEUED,
                Optional.of(Instant.parse("2022-01-01T05:00:00.000Z")));
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j0);
        qu.scheduleJob(j1);

        var jobs = qu.pullJobs(Instant.parse("2022-01-01T05:00:00.000Z"), 10);

        Assertions.assertEquals(1, jobs.size());
        Assertions.assertEquals(new Job<>(j0.data(), j0.id(), JobStatus.IN_PROGRESS, j0.scheduledFor()), jobs.get(0));
    }

    @Test
    public void InMemoryJobQueue_RespectsMax() {
        var j0 = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var j1 = new Job<>("Test1", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var j2 = new Job<>("Test2", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j0);
        qu.scheduleJob(j1);
        qu.scheduleJob(j2);

        var jobs = qu.pullJobs(Instant.MAX, 2);

        Assertions.assertEquals(2, jobs.size());
        Assertions.assertEquals(new Job<>(j0.data(), j0.id(), JobStatus.IN_PROGRESS, j0.scheduledFor()), jobs.get(0));
        Assertions.assertEquals(new Job<>(j1.data(), j1.id(), JobStatus.IN_PROGRESS, j1.scheduledFor()), jobs.get(1));
    }

    @Test
    public void InMemoryJobQueue_MarksAsComplete() {
        var j0 = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j0);
        qu.completeJob(j0.id());

        var job = qu.getJob(j0.id());

        Assertions.assertEquals(JobStatus.COMPLETED, job.get().getStatus());
    }

    @Test
    public void InMemoryJobQueue_MarksAsFailed() {
        var j0 = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j0);
        qu.markJobAsFailed(j0.id());

        var job = qu.getJob(j0.id());

        Assertions.assertEquals(JobStatus.FAILED, job.get().getStatus());
    }
}