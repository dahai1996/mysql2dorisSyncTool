package builder;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wfs
 */
public class StreamEnvBuilder {
    private final StreamExecutionEnvironment env;

    public StreamEnvBuilder() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamEnvBuilder builder() {
        return new StreamEnvBuilder();
    }

    public StreamEnvBuilder setCheckpointInterval(long checkpointInterval) {
        env.enableCheckpointing(checkpointInterval);
        return this;
    }

    public StreamEnvBuilder setCheckpointingMode(CheckpointingMode checkpointingMode) {
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        return this;
    }

    public StreamEnvBuilder setCheckpointTimeout(long checkpointTimeout) {
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        return this;
    }

    public StreamEnvBuilder setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        return this;
    }

    public StreamEnvBuilder setTolerableCheckpointFailureNumber(
            int tolerableCheckpointFailureNumber) {
        env.getCheckpointConfig()
                .setTolerableCheckpointFailureNumber(tolerableCheckpointFailureNumber);
        return this;
    }

    public StreamEnvBuilder setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        return this;
    }

    public StreamEnvBuilder setRestartStrategy(
            RestartStrategies.RestartStrategyConfiguration restartStrategy) {
        env.setRestartStrategy(restartStrategy);
        return this;
    }

    public StreamEnvBuilder setDefaultRestartStrategy(
            int failureRate, Time failureInterval, Time delayInterval) {
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(failureRate, failureInterval, delayInterval));
        return this;
    }

    public StreamEnvBuilder setHashMapStateBackend(int maxStateSizeMb) {
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        new JobManagerCheckpointStorage(maxStateSizeMb * 1024 * 1024));
        return this;
    }

    public StreamEnvBuilder setFileBackend(String path) {
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(path));
        return this;
    }

    public StreamEnvBuilder setParallelism(int parallelism) {
        env.setParallelism(parallelism);
        return this;
    }

    public StreamExecutionEnvironment build() {
        return env;
    }
}
