package org.rafiss.connpools;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class Main {

    public static void main(String[] args) {
//        System.setProperty("com.zaxxer.hikari.aliveBypassWindowMs", "1");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable");
        config.setUsername("root");
//        config.setPassword("p@55word");
        config.setConnectionTimeout(10000);
//        config.setConnectionTestQuery("select 1;");

        HikariDataSource ds = new HikariDataSource(config);
        Jdbi jdbi = Jdbi.create(ds);
        LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<>();
        ExecutorService pool = new ThreadPoolExecutor(10, 10,
                0L, TimeUnit.MILLISECONDS,
                q, new ThreadPoolExecutor.CallerRunsPolicy());
        List<Future<Boolean>> fs = new ArrayList<>();

        for (int i = 0; i < 280; i++) {
            System.out.println(i);
            Future<Boolean> f = pool.submit(() -> {

                try (Connection conn = ds.getConnection()) {
                    Statement s = conn.createStatement();
                    ResultSet r = s.executeQuery("select pg_sleep(0.5)");
                    r.next();
                    boolean bo = r.getBoolean(1);
                    r.close();
                    if (bo) {
                        return bo;
                    } else {
                        throw new RuntimeException("impossible");
                    }
                } catch (Exception throwables) {
                    throw new RuntimeException(throwables);
                }

//                Boolean b = jdbi.withHandle(handle -> {
//                    try {
//                        System.out.println("isValid before: " + handle.getConnection().isValid(100));
//                    } catch (SQLException se) {
//                        se.printStackTrace();
//                    }
//                    handle.execute("select 1;");
//                    Thread.sleep(15000);
//                    Boolean r =  handle.createQuery("select pg_sleep(0.5);")
//                            .mapTo(Boolean.class)
//                            .first();
//                    return r;
//                    try {
//                        System.out.println("isValid after: " + handle.getConnection().isValid(100));
//                    } catch (SQLException se) {
//                        se.printStackTrace();
//                    }
//                });
//                assertThat(b, is(true));
//                return b;
            });
            fs.add(f);
        }

        int execErrors = 0;
        int timeoutErrors = 0;
        for (int i = 0; i < fs.size(); i++) {
            Future<Boolean> f = fs.get(i);
            try {
                Boolean b = f.get(100000, TimeUnit.MILLISECONDS);
                int activeConnections = ds.getHikariPoolMXBean().getActiveConnections();
                int totalConnections = ds.getHikariPoolMXBean().getTotalConnections();
                System.out.println(b + " " + i + " queueSize=" + q.size() + " activeConns=" + activeConnections + " totalConns=" + totalConnections);
            } catch (InterruptedException e) {
                System.out.println("interrupted");
            } catch (ExecutionException e) {
                execErrors++;
                e.getCause().printStackTrace();
            } catch (TimeoutException e) {
                timeoutErrors++;
                e.printStackTrace();
            }
        }
        pool.shutdown();

        System.out.printf("execErrors=%d timeoutErrors=%d\n", execErrors, timeoutErrors);
    }
}
