package de.garrus.cloudnet.couchbase;

import com.couchbase.client.core.cnc.LoggingEventConsumer;
import de.dytanic.cloudnet.common.logging.ILogger;
import de.dytanic.cloudnet.common.logging.LogLevel;
import de.dytanic.cloudnet.driver.CloudNetDriver;

import java.util.regex.Matcher;

public class CouchbaseCloudNetLogger implements LoggingEventConsumer.Logger {
    private final ILogger logger;
    private static final String MSG_PREFIX = "[Couchbase] ";

    public CouchbaseCloudNetLogger() {
        logger = CloudNetDriver.getInstance().getLogger();
    }

    @Override
    public String getName() {
        return "Cloudnet";
    }

    @Override
    public boolean isTraceEnabled() {
        return LogLevel.ALL.getLevel() <= logger.getLevel();
    }

    @Override
    public void trace(String msg) {
        logger.log(LogLevel.ALL, MSG_PREFIX + msg);
    }

    @Override
    public void trace(String format, Object... arguments) {
        logger.log(LogLevel.ALL, formatHelper(MSG_PREFIX + format, arguments));
    }

    @Override
    public void trace(String msg, Throwable t) {
        logger.log(LogLevel.ALL, MSG_PREFIX + msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return LogLevel.DEBUG.getLevel() <= logger.getLevel();
    }

    @Override
    public void debug(String msg) {
        logger.debug(MSG_PREFIX + msg);
    }

    @Override
    public void debug(String format, Object... arguments) {
        logger.debug(formatHelper(MSG_PREFIX + format, arguments));
    }

    @Override
    public void debug(String msg, Throwable t) {
        logger.debug(msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return LogLevel.INFO.getLevel() <= logger.getLevel();

    }

    @Override
    public void info(String msg) {
        logger.info(MSG_PREFIX + msg);
    }

    @Override
    public void info(String format, Object... arguments) {
        logger.info(formatHelper(MSG_PREFIX + format, arguments));
    }

    @Override
    public void info(String msg, Throwable t) {
        logger.log(LogLevel.INFO, MSG_PREFIX + msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return LogLevel.WARNING.getLevel() <= logger.getLevel();
    }

    @Override
    public void warn(String msg) {
        logger.warning(MSG_PREFIX + msg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        logger.warning(formatHelper(MSG_PREFIX + format, arguments));
    }

    @Override
    public void warn(String msg, Throwable t) {
        logger.warning(msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return LogLevel.IMPORTANT.getLevel() <= logger.getLevel();
    }

    @Override
    public void error(String msg) {
        logger.error(MSG_PREFIX + msg);
    }

    @Override
    public void error(String format, Object... arguments) {
        logger.error(formatHelper(MSG_PREFIX + format, arguments));
    }

    @Override
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }

    private String formatHelper(final String from, final Object... arguments) {
        if (from != null) {
            String computed = from;
            if (arguments != null && arguments.length != 0) {
                for (Object argument : arguments) {
                    computed = computed.replaceFirst(
                            "\\{\\}", Matcher.quoteReplacement(argument.toString())
                    );
                }
            }
            return computed;
        }
        return null;
    }
}
