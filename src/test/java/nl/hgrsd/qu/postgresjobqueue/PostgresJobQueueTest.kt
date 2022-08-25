package nl.hgrsd.qu.postgresjobqueue

import kotlinx.serialization.Serializable
import nl.hgrsd.qu.JobQueue.Job
import nl.hgrsd.qu.JobQueue.JobStatus
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

fun getConnection(): Connection {
    val dburl = System.getenv("QU_DBURL");
    val pw = System.getenv("QU_DBPASSWORD");
    val un = System.getenv("QU_DBUSERNAME");
    return DriverManager.getConnection("jdbc:${dburl}", un, pw);
}

@Serializable
data class TestPayload(val content: String)

internal class PostgresJobQueueTest {
    @Test
    fun postgresJobQueue_canScheduleJob() {
        val conn = getConnection();
        val qu = PostgresJobQueue(conn, TestPayload.serializer());
        val job = Job(TestPayload("hi"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        qu.scheduleJob(job);
        val scheduled = qu.getJob(job.id()).get()
        Assertions.assertEquals(scheduled.id(), job.id())
        Assertions.assertEquals(scheduled.data(), job.data())
        Assertions.assertEquals(scheduled.scheduledFor(), job.scheduledFor())
        Assertions.assertEquals(scheduled.status, job.status)
    }

    @Test
    fun postgresJobQueue_returnsEmpty() {
        val conn = getConnection();
        val qu = PostgresJobQueue(conn, TestPayload.serializer());
        val empty = qu.getJob(UUID.randomUUID());
        Assertions.assertTrue(empty.isEmpty);
    }

    @Test
    fun postgresJobQueue_deletesJob() {
        val conn = getConnection();
        val qu = PostgresJobQueue(conn, TestPayload.serializer());
        val job = Job(TestPayload("hi"), UUID.randomUUID(), JobStatus.QUEUED, Optional.empty());
        qu.scheduleJob(job);
        qu.deleteJob(job.id())
        val scheduled = qu.getJob(job.id());
        Assertions.assertTrue(scheduled.isEmpty)
    }

}