package net.surpin.benchbase;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public final class HiveExecuteDriver implements Driver {
    private static final String URL_PREFIX = "jdbc:hiveexec:";
    private final Driver delegate = new org.apache.hive.jdbc.HiveDriver();

    static {
        try {
            DriverManager.registerDriver(new HiveExecuteDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Connection connection = delegate.connect(toHiveUrl(url), info);
        return wrapConnection(connection);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(toHiveUrl(url), info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    private static String toHiveUrl(String url) {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            return url;
        }
        return "jdbc:" + url.substring(URL_PREFIX.length());
    }

    private static Connection wrapConnection(Connection connection) {
        InvocationHandler handler = new ConnectionHandler(connection);
        return (Connection) Proxy.newProxyInstance(
                HiveExecuteDriver.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                handler);
    }

    private static final class ConnectionHandler implements InvocationHandler {
        private final Connection delegate;

        private ConnectionHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();

            switch (name) {
                case "rollback":
                case "commit":
                case "setAutoCommit":
                case "setTransactionIsolation":
                    return null;
                case "getAutoCommit":
                    return Boolean.TRUE;
                case "getTransactionIsolation":
                    return Connection.TRANSACTION_NONE;
                case "unwrap":
                    return unwrap(args);
                case "isWrapperFor":
                    return isWrapperFor(args);
                default:
                    break;
            }

            Object[] forwardedArgs = rewriteSqlArgumentIfNeeded(name, args);
            try {
                return method.invoke(delegate, forwardedArgs);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        private Object unwrap(Object[] args) throws SQLException {
            Class<?> target = (Class<?>) args[0];
            if (target.isInstance(delegate)) {
                return delegate;
            }
            return delegate.unwrap(target);
        }

        private Object isWrapperFor(Object[] args) throws SQLException {
            Class<?> target = (Class<?>) args[0];
            return target.isInstance(delegate) || delegate.isWrapperFor(target);
        }

        private static Object[] rewriteSqlArgumentIfNeeded(String methodName, Object[] args) {
            if (!methodName.startsWith("prepare") || args == null || args.length == 0 || !(args[0] instanceof String)) {
                return args;
            }

            Object[] rewritten = args.clone();
            rewritten[0] = rewriteSql((String) args[0]);
            return rewritten;
        }

        private static String rewriteSql(String sql) {
            String result = sql;
            result = result.replaceAll("(?i)\\?\\s*::\\s*date", "CAST(? AS DATE)");
            result = result.replaceAll("(?i)'(\\d{4}-\\d{2}-\\d{2})'\\s*::\\s*date", "DATE '$1'");
            result = result.replaceAll("(?i)\\?\\s*::\\s*decimal(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?", "CAST(? AS DECIMAL)");
            result = result.replaceAll("(?i)'(\\d+\\.?\\d*)'\\s*::\\s*decimal(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*\\d+\\s*)?\\))?", "$1");
            result = result.replaceAll("(?i)interval\\s+'([^']+)'\\s+(year|month|day|hour|minute|second)s?", "INTERVAL '$1' $2");
            return result;
        }
    }
}
