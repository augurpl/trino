/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.plugin.base.util.LockUtils.CloseableLock;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.LockUtils.closeable;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class LazyConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;

    @Inject
    public LazyConnectionFactory(RetryingConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return new LazyConnection(() -> delegate.openConnection(session));
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }

    private static final class LazyConnection
            extends ForwardingConnection
    {
        private final ReentrantLock lock = new ReentrantLock();

        private final SqlSupplier<Connection> connectionSupplier;
        @Nullable
        @GuardedBy("lock")
        private Connection connection;
        @GuardedBy("lock")
        private boolean closed;

        public LazyConnection(SqlSupplier<Connection> connectionSupplier)
        {
            this.connectionSupplier = requireNonNull(connectionSupplier, "connectionSupplier is null");
        }

        @Override
        protected Connection delegate()
                throws SQLException
        {
            try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
                checkState(!closed, "Connection is already closed");
                if (connection == null) {
                    connection = requireNonNull(connectionSupplier.get(), "connectionSupplier.get() is null");
                }
                return connection;
            }
        }

        @Override
        public void close()
                throws SQLException
        {
            try (CloseableLock<ReentrantLock> ignored = closeable(lock)) {
                closed = true;
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    @FunctionalInterface
    private interface SqlSupplier<T>
    {
        T get()
                throws SQLException;
    }
}
