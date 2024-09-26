package tech.ydb.demo.q1k;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

/**
 *
 * @author zinal
 */
public class Ydb1kQuery implements AutoCloseable {
    
    public static final String PROP_URL = "url";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";

    private final Connection connection;

    public Ydb1kQuery(Properties props) throws SQLException {
        if (props.containsKey(PROP_USER)) {
            connection = DriverManager.getConnection(
                    props.getProperty(PROP_URL),
                    props.getProperty(PROP_USER),
                    props.getProperty(PROP_PASSWORD)
            );
        } else {
            connection = DriverManager.getConnection(
                    props.getProperty(PROP_URL)
            );
        }
        System.out.println("...Connected.");
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch(SQLException sqe) {}
        System.out.println("...Disconnected.");
    }

    public void run() throws SQLException {
        List<Long> times = new ArrayList<>();

        for (int i=0; i<10000; ++i) {
            long time = call();
            times.add(time);
        }

        List<Long> sortedTimes = times.stream()
          .sorted()
          .collect(Collectors.toList());

        long sum = sortedTimes.stream().collect(Collectors.summingLong(Long::longValue));
        System.out.format("Average : %.2f", ((double)sum) / ((double)sortedTimes.size()));
        System.out.println();
        System.out.format("Perc 50 : %d", getPercentile(sortedTimes, 50.0));
        System.out.println();
        System.out.format("Perc 75 : %d", getPercentile(sortedTimes, 75.0));
        System.out.println();
        System.out.format("Perc 90 : %d", getPercentile(sortedTimes, 90.0));
        System.out.println();
        System.out.format("Perc 95 : %d", getPercentile(sortedTimes, 95.0));
        System.out.println();
        System.out.format("Perc 99 : %d", getPercentile(sortedTimes, 99.0));
        System.out.println();
    }

    private long call() throws SQLException {
        String input = makeString(1024);
        String output = "";
        Instant start = Instant.now();
        try (PreparedStatement ps = connection.prepareStatement("SELECT ?")) {
            ps.setString(1, input);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    output = rs.getString(1);
                }
            }
        }
        Instant finish = Instant.now();
        if (! input.equals(output)) {
            throw new RuntimeException("Input differs");
        }
        return ChronoUnit.MICROS.between(start, finish);
    }

    static Long getPercentile(List<Long> input, double percentile) {
        int rank = percentile == 0 ? 1 : (int) Math.ceil(percentile / 100.0 * input.size());
        return input.get(rank - 1);
    }

    static String makeString(int targetStringLength) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        return new Random().ints(leftLimit, rightLimit + 1)
          .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
          .limit(targetStringLength)
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            String propFileName = "connection.properties";
            if (args.length > 0) {
                propFileName = args[0];
            }
            try (FileInputStream fis = new FileInputStream(propFileName)) {
                props.load(fis);
            }
            try (Ydb1kQuery instance = new Ydb1kQuery(props)) {
                instance.run();
            }
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
