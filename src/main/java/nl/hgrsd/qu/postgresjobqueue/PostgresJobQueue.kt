package nl.hgrsd.qu.postgresjobqueue

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import nl.hgrsd.qu.JobQueue.Job
import nl.hgrsd.qu.JobQueue.JobQueue
import nl.hgrsd.qu.JobQueue.JobStatus
import org.postgresql.util.PGobject
import java.lang.Error
import java.lang.Exception
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.Optional
import javax.sql.DataSource

class PostgresJobQueue<T>(private val ds: DataSource, private val serializer: KSerializer<T>) : JobQueue<T> {
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

    private fun <T> runTx(fn: (Connection) -> T): T {
        val conn = ds.connection
        conn.autoCommit = false
        try {
            val result = fn(conn)
            conn.commit()
            return result
        } catch (e: Exception) {
            conn.rollback()
            throw e
        } finally {
            conn.close()
        }
    }

    override fun scheduleJob(job: Job<T>) {
        runTx {
            val s = it.prepareStatement(
                """
                INSERT INTO qu 
                (job_id, scheduled_for, status, payload)
                VALUES (?, ?, ?, ?);
            """
            )
            s.setObject(1, job.id())
            job.scheduledFor()
                .ifPresentOrElse(
                    { instant -> s.setTimestamp(2, Timestamp.from(instant)) },
                    { s.setNull(2, java.sql.Types.NULL) })
            s.setString(3, job.status.toString())
            val payload = PGobject()
            payload.type = "jsonb"
            payload.value = Json.encodeToString(serializer, job.data())
            s.setObject(4, payload)
            s.execute()
        }
    }

    override fun deleteJob(id: UUID) {
        runTx {
            val s = it.prepareStatement("DELETE FROM qu WHERE job_id = ?")
            s.setObject(1, id)
            s.execute()
        }
    }

    override fun pullJobs(cutOff: Instant, maxJobs: Int): MutableList<Job<T>> {
        return runTx {
            val s = it.prepareStatement(
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
            return@runTx jobs
        }
    }

    override fun completeJob(id: UUID) {
        runTx {
            val s = it.prepareStatement("UPDATE qu SET status = ? WHERE job_id = ?;")
            s.setString(1, JobStatus.COMPLETED.toString())
            s.setObject(2, id)
            s.execute()
        }
    }

    override fun markJobAsFailed(id: UUID) {
        runTx { conn ->
            val s = conn.prepareStatement("UPDATE qu SET status = ? WHERE job_id = ?;")
            s.setString(1, JobStatus.FAILED.toString())
            s.setObject(2, id)
            s.execute()
        }
    }

    override fun getJob(id: UUID): Optional<Job<T>> {
        return runTx {
            val s = it.prepareStatement("SELECT * FROM qu WHERE job_id = ?")
            s.setObject(1, id)

            val rs = s.executeQuery()
            if (!rs.next()) {
                return@runTx Optional.empty()
            }
            return@runTx Optional.of(deserializeJob(rs))
        }
    }

}