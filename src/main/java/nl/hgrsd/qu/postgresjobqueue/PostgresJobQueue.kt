package nl.hgrsd.qu.postgresjobqueue

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import nl.hgrsd.qu.JobQueue.Job
import nl.hgrsd.qu.JobQueue.JobQueue
import nl.hgrsd.qu.JobQueue.JobStatus
import org.postgresql.util.PGobject
import java.lang.Error
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.Optional

class PostgresJobQueue<T>(private val conn: Connection, private val serializer: KSerializer<T>) : JobQueue<T> {
    private fun deserializeJob(rs: ResultSet): Job<T> {
        val obj: PGobject = rs.getObject("payload") as PGobject
        val payload = Json.decodeFromString(serializer, obj.value.orEmpty())
        val status: JobStatus = when (rs.getString("status")) {
            "QUEUED" -> JobStatus.QUEUED
            "IN_PROGRESS" -> JobStatus.IN_PROGRESS
            "FAILED" -> JobStatus.FAILED
            "COMPLETED" -> JobStatus.COMPLETED
            else -> {
                throw Error("Unexpected status")
            }
        }
        val scheduledFor = Optional.ofNullable(rs.getTimestamp("scheduled_for")).map { it.toInstant() }
        return Job(payload, rs.getObject("job_id") as UUID, status, scheduledFor)
    }

    override fun scheduleJob(job: Job<T>) {
        val s = conn.prepareStatement(
            """
                INSERT INTO qu 
                (job_id, scheduled_for, status, payload)
                VALUES (?, ?, ?, ?);
            """
        )
        s.setObject(1, job.id())
        job.scheduledFor()
            .ifPresentOrElse({ s.setTimestamp(2, Timestamp.from(it)) }, { s.setNull(2, java.sql.Types.NULL) })
        s.setString(3, job.status.toString())
        val payload = PGobject()
        payload.type = "jsonb"
        payload.value = Json.encodeToString(serializer, job.data())
        s.setObject(4, payload)
        s.execute()
    }

    override fun deleteJob(id: UUID) {
        val s = conn.prepareStatement("DELETE FROM qu WHERE job_id = ?")
        s.setObject(1, id)
        s.execute()
    }

    override fun pullJobs(cutOff: Instant, maxJobs: Int): MutableList<Job<T>> {
        val s = conn.prepareStatement(
            """
                UPDATE qu SET STATUS = ?
                WHERE internal_id IN (
                    SELECT internal_id 
                    FROM qu 
                    WHERE STATUS = ?
                    AND (scheduled_for IS NULL OR scheduled_for <= ?)
                    ORDER BY internal_id
                    LIMIT ?
                ) RETURNING *;
                """
        )
        s.setString(1, JobStatus.IN_PROGRESS.toString())
        s.setString(2, JobStatus.QUEUED.toString())
        s.setTimestamp(3, Timestamp.from(cutOff))
        s.setInt(4, maxJobs)

        val jobs = mutableListOf<Job<T>>()
        val rs = s.executeQuery()
        while (rs.next()) {
            jobs.add(deserializeJob(rs))
        }
        return jobs
    }

    override fun completeJob(id: UUID) {
        val s = conn.prepareStatement("UPDATE qu SET status = ? WHERE job_id = ?;")
        s.setString(1, JobStatus.COMPLETED.toString())
        s.setObject(2, id)
        s.execute()
    }

    override fun markJobAsFailed(id: UUID) {
        val s = conn.prepareStatement("UPDATE qu SET status = ? WHERE job_id = ?;")
        s.setString(1, JobStatus.FAILED.toString())
        s.setObject(2, id)
        s.execute()
    }

    override fun getJob(id: UUID): Optional<Job<T>> {
        val s = conn.prepareStatement("SELECT * FROM qu WHERE job_id = ?")
        s.setObject(1, id)

        val rs = s.executeQuery()
        if (!rs.next()) {
            return Optional.empty()
        }
        return Optional.of(deserializeJob(rs))
    }

}