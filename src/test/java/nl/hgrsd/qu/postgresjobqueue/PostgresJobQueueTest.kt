package nl.hgrsd.qu.postgresjobqueue

import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.Serializable
import nl.hgrsd.qu.JobQueue.Job
import nl.hgrsd.qu.JobQueue.JobStatus
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.time.Instant
import java.util.*
import javax.sql.DataSource

fun getDataSource(): DataSource {
    val pw = System.getenv("QU_DBPASSWORD")
    val un = System.getenv("QU_DBUSERNAME")
    val dburl = System.getenv("QU_DBURL")
    val ds = HikariDataSource()
    ds.dataSourceClassName = "org.postgresql.ds.PGSimpleDataSource"
    ds.jdbcUrl = "jdbc:${dburl}"
    ds.username = un
    ds.password = pw
    return ds
}

@Serializable
data class TestPayload(val content: String)

internal class PostgresJobQueueTest {
    private val ds = getDataSource()


    @BeforeEach
    fun clear() {
        val st = ds.connection.createStatement()
        st.execute("DELETE FROM qu;")
        ds.connection.close()
    }

    @Test
    fun postgresJobQueue_canScheduleJob() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val job = Job(TestPayload("hi"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty())
        qu.scheduleJob(job)
        val scheduled = qu.getJob(job.id()).get()
        Assertions.assertEquals(scheduled.id(), job.id())
        Assertions.assertEquals(scheduled.data(), job.data())
        Assertions.assertEquals(scheduled.scheduledFor(), job.scheduledFor())
        Assertions.assertEquals(scheduled.status, job.status)
    }

    @Test
    fun postgresJobQueue_returnsEmpty() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val empty = qu.getJob(UUID.randomUUID())
        Assertions.assertTrue(empty.isEmpty)
    }

    @Test
    fun postgresJobQueue_deletesJob() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val job = Job(TestPayload("hi"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty())
        qu.scheduleJob(job)
        qu.deleteJob(job.id())
        val scheduled = qu.getJob(job.id())
        Assertions.assertTrue(scheduled.isEmpty)
    }

    @Test
    fun postgresJobQueue_pullsJobsInOrder() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val j0 = Job(TestPayload("test"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty())
        val j1 = Job(TestPayload("test1"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty())
        val j2 = Job(TestPayload("test2"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty())
        qu.scheduleJob(j0)
        qu.scheduleJob(j1)
        qu.scheduleJob(j2)

        val jobs = qu.pullJobs(Instant.now().plusMillis(1000), 10)

        Assertions.assertEquals(3, jobs.size)
        Assertions.assertEquals(
            Job(
                j0.data(),
                j0.id(),
                JobStatus.IN_PROGRESS,
                j0.scheduledFor()
            ), jobs[0]
        )
        Assertions.assertEquals(
            Job(
                j1.data(),
                j1.id(),
                JobStatus.IN_PROGRESS,
                j1.scheduledFor()
            ), jobs[1]
        )
        Assertions.assertEquals(
            Job(
                j2.data(),
                j2.id(),
                JobStatus.IN_PROGRESS,
                j2.scheduledFor()
            ), jobs[2]
        )
    }

    @Test
    fun postgresJobQueue_returnsJobsUsingCutoff() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val j0 = Job(
            TestPayload("should not return"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.of(Instant.parse("2022-01-01T00:00:00.000Z"))
        )
        val j1 = Job(
            TestPayload("should return"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.of(Instant.parse("2021-12-31T23:59:59.999Z"))
        )
        qu.scheduleJob(j0)
        qu.scheduleJob(j1)

        val jobs = qu.pullJobs(Instant.parse("2021-12-31T23:59:59.999Z"), 10)

        Assertions.assertEquals(1, jobs.size)
        Assertions.assertEquals(
            Job(
                j1.data(),
                j1.id(),
                JobStatus.IN_PROGRESS,
                j1.scheduledFor()
            ), jobs[0]
        )
    }

    @Test
    fun postgresJobQueue_respectsMax() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val j0 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        val j1 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        val j2 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        qu.scheduleJob(j0)
        qu.scheduleJob(j1)
        qu.scheduleJob(j2)

        val jobs = qu.pullJobs(Instant.now(), 2)

        Assertions.assertEquals(2, jobs.size)
        Assertions.assertEquals(
            Job(
                j0.data(),
                j0.id(),
                JobStatus.IN_PROGRESS,
                j0.scheduledFor()
            ), jobs[0]
        )
        Assertions.assertEquals(
            Job(
                j1.data(),
                j1.id(),
                JobStatus.IN_PROGRESS,
                j1.scheduledFor()
            ), jobs[1]
        )
    }

    @Test
    fun postgresJobQueue_doesNotReturnInProgressJobs() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val j0 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        val j1 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        val j2 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        qu.scheduleJob(j0)
        qu.scheduleJob(j1)
        qu.scheduleJob(j2)

        val jobs1 = qu.pullJobs(Instant.now(), 1)

        Assertions.assertEquals(1, jobs1.size)

        val jobs2 = qu.pullJobs(Instant.now(), 10)
        Assertions.assertEquals(2, jobs2.size)
    }

    @Test
    fun postgresJobQueue_marksAsCompleted() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val j0 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        qu.scheduleJob(j0)
        qu.completeJob(j0.id())
        val job = qu.getJob(j0.id()).get()
        Assertions.assertEquals(JobStatus.COMPLETED, job.status)
    }

    @Test
    fun postgresJobQueue_marksAsFailed() {
        val qu = PostgresJobQueue(ds, TestPayload.serializer())
        val j0 = Job(
            TestPayload("test"),
            UUID.randomUUID(),
            JobStatus.QUEUED,
            Optional.empty()
        )
        qu.scheduleJob(j0)
        qu.markJobAsFailed(j0.id())
        val job = qu.getJob(j0.id()).get()
        Assertions.assertEquals(JobStatus.FAILED, job.status)
    }

}