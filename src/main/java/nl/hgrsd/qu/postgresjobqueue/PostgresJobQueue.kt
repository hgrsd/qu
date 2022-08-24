package nl.hgrsd.qu.postgresjobqueue

import nl.hgrsd.qu.JobQueue.Job
import nl.hgrsd.qu.JobQueue.JobQueue
import java.time.Instant
import java.util.UUID
import java.util.Optional

class PostgresJobQueue<T>: JobQueue<T> {
    override fun scheduleJob(job: Job<T>) {
        TODO("Not yet implemented")
    }

    override fun deleteJob(id: UUID) {
        TODO("Not yet implemented")
    }

    override fun pullJobs(cutOff: Instant, maxJobs: Int): MutableList<Job<T>> {
        TODO("Not yet implemented")
    }

    override fun completeJob(id: UUID) {
        TODO("Not yet implemented")
    }

    override fun markJobAsFailed(id: UUID) {
        TODO("Not yet implemented")
    }

    override fun getJob(id: UUID): Optional<Job<T>> {
        TODO("Not yet implemented")
    }

}