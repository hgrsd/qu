package nl.hgrsd.qu.postgresjobqueue

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import nl.hgrsd.qu.JobQueue.Job
import nl.hgrsd.qu.JobQueue.JobQueue
import nl.hgrsd.qu.JobQueue.JobStatus
import org.postgresql.util.PGobject
import java.lang.Error
import java.sql.Connection
import java.sql.Date
import java.sql.ResultSet
import java.time.Instant
import java.util.UUID
import java.util.Optional

class PostgresJobQueue<T>(private val conn: Connection, private val serializer: KSerializer<T>) : JobQueue<T> {
    private fun deserializeJob(rs: ResultSet): Job<T> {
        val obj: PGobject = rs.getObject("payload") as PGobject;
        val payload = Json.decodeFromString(serializer, obj.value.orEmpty());
        val status: JobStatus = when (rs.getString("status")) {
            "QUEUED" -> JobStatus.QUEUED;
            "IN_PROGRESS" -> JobStatus.IN_PROGRESS;
            "FAILED" -> JobStatus.FAILED;
            "COMPLETED" -> JobStatus.COMPLETED;
            else -> {
                throw Error("Unexpected status");
            }
        }
        val scheduledFor = Optional.ofNullable(rs.getDate("scheduled_for")).map { it.toInstant() };
        return Job(payload, rs.getObject("job_id") as UUID, status, scheduledFor)
    }
    override fun scheduleJob(job: Job<T>) {
        val s = conn.prepareStatement(
            "INSERT INTO qu (job_id, scheduled_for, status, payload)" +
                    "VALUES (?, ?, ?, ?);"
        );
        s.setObject(1, job.id());
        job.scheduledFor()
            .ifPresentOrElse({ s.setDate(2, Date(it.epochSecond)) }, { s.setNull(2, java.sql.Types.NULL) });
        s.setString(3, job.status.toString());
        val payload = PGobject()
        payload.type = "jsonb"
        payload.value = Json.encodeToString(serializer, job.data())
        s.setObject(4, payload);
        s.execute();
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
        val s = conn.prepareStatement("SELECT * FROM qu WHERE job_id = ?");
        s.setObject(1, id);

        val rs = s.executeQuery();
        if (!rs.next()) {
            return Optional.empty()
        }
        return Optional.of(deserializeJob(rs))
    }

}