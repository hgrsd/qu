package nl.hgrsd.qu.InMemoryJobQueue;

import nl.hgrsd.qu.JobQueue.Job;
import nl.hgrsd.qu.JobQueue.JobStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

class InMemoryJobQueueTest {
    @Test
    void InMemoryJobQueue_SchedulesJob() {
        var j = new Job<>("Test", UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        var qu = new InMemoryJobQueue<String>();
        qu.scheduleJob(j);

        var job = qu.getJob(j.id());
        Assertions.assertEquals(j, job.get());
    }

    @Test
    void InMemoryJobQueue_ReturnsEmpty() {
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

}