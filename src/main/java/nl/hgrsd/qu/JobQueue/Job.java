package nl.hgrsd.qu.JobQueue;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class Job<T> {
    private final T data;
    private final UUID id;
    private JobStatus status;
    private final Optional<Instant> scheduledFor;

    public Job(T data, UUID id, JobStatus status, Optional<Instant> scheduledFor) {
        this.data = data;
        this.id = id;
        this.status = status;
        this.scheduledFor = scheduledFor;
    }

    public T data() {
        return data;
    }

    public UUID id() {
        return id;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public Optional<Instant> scheduledFor() {
        return scheduledFor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Job) obj;
        return Objects.equals(this.data, that.data) &&
                Objects.equals(this.id, that.id) &&
                Objects.equals(this.status, that.status) &&
                Objects.equals(this.scheduledFor, that.scheduledFor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, id, status, scheduledFor);
    }

    @Override
    public String toString() {
        return "Job[" +
                "data=" + data + ", " +
                "id=" + id + ", " +
                "status=" + status + ", " +
                "scheduledFor=" + scheduledFor + ']';
    }

}
