package io.rwa.server.tx;

import io.rwa.server.common.ApiException;
import io.rwa.server.config.RwaProperties;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import javax.sql.DataSource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class NonceLockManager {

    @FunctionalInterface
    public interface LockedCallback<T> {
        T run() throws Exception;
    }

    private final DataSource dataSource;
    private final RwaProperties properties;

    public NonceLockManager(DataSource dataSource, RwaProperties properties) {
        this.dataSource = dataSource;
        this.properties = properties;
    }

    public <T> T withAddressLock(String fromAddress, LockedCallback<T> callback) {
        try (Connection connection = dataSource.getConnection()) {
            boolean acquired = tryAcquire(connection, fromAddress, Duration.ofMillis(properties.getNonceLockTimeoutMs()));
            if (!acquired) {
                throw new ApiException(HttpStatus.CONFLICT, "Address is busy");
            }
            try {
                return callback.run();
            } finally {
                release(connection, fromAddress);
            }
        } catch (ApiException e) {
            throw e;
        } catch (Exception e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "Nonce lock failure: " + e.getMessage());
        }
    }

    private boolean tryAcquire(Connection connection, String address, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (PreparedStatement ps = connection.prepareStatement(
                "SELECT pg_try_advisory_lock(hashtext(lower(?)))")) {
                ps.setString(1, address);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getBoolean(1)) {
                        return true;
                    }
                }
            }
            Thread.sleep(50L);
        }
        return false;
    }

    private void release(Connection connection, String address) {
        try (PreparedStatement ps = connection.prepareStatement(
            "SELECT pg_advisory_unlock(hashtext(lower(?)))")) {
            ps.setString(1, address);
            ps.execute();
        } catch (Exception ignored) {
            // best-effort unlock
        }
    }
}
