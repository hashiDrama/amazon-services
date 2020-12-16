package main;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class AwsS3Util {
  private static final String CASSANDRA = "cassandra/";
  private static String env = "staging";

  public AmazonS3ClientBuilder getS3ClientBuilder() {
    AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
    s3ClientBuilder.setRegion("us-west-2");
    return s3ClientBuilder;
  }

  public AmazonS3 getAwsS3Interface() {
    return getS3ClientBuilder().build();
  }

  public Optional<BigInteger> getS3Storage(String customerId) {
    AmazonS3 s3 = getAwsS3Interface();
    String bucketName = "tala-" + env + "-archive";
    List<S3ObjectSummary> objects = s3.listObjectsV2(bucketName).getObjectSummaries();
    long storage = objects.parallelStream()
        .filter(
            object -> object.getKey().contains("cassandra") && object.getKey().contains(customerId))
        .mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public Optional<BigInteger> getS3StorageInInterval(String customerId, DateTime from,
      DateTime to) {
    AmazonS3 s3 = getAwsS3Interface();
    String bucketName = "tala-" + env + "-archive";
    List<S3ObjectSummary> objects = s3.listObjectsV2(bucketName).getObjectSummaries();
    long storage = objects.parallelStream().filter(object -> {
      if (object.getKey().contains("cassandra") && object.getKey().contains(customerId)) {
        DateTime date = DateTime.parse(
            object.getKey().substring(object.getKey().indexOf("cassandra/") + CASSANDRA.length(),
                object.getKey().indexOf(customerId) - 1));
        if (date.isAfter(from.toInstant()) && date.isBefore(to.toInstant()))
          return true;
      }
      return false;
    }).mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public static void main(String[] args) {
    ArgumentParser parser =
        ArgumentParsers.newArgumentParser("s3-storage").defaultHelp(true).description("S3 storage");
    parser.addArgument("--env")
        .help("Specify the environment.Has to be one of [ integration, staging, production ]")
        .type(String.class).setDefault("staging");

    parser.addArgument("--customer.id").help("Customer id").type(String.class).required(true);

    parser.addArgument("--from.date")
        .help("Specify the date after which records are needed. Format should be -> yyyy-mm-dd")
        .type(String.class).required(false);

    parser.addArgument("--to.date")
        .help("Specify the date upto which records are needed. Format should be -> yyyy-mm-dd")
        .type(String.class).required(false);

    Namespace ns;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return;
    }
    env = ns.getString("env");
    String customerId = ns.getString("customer.id");
    String from = ns.getString("from.date");
    String to = ns.getString("to.date");

    AwsS3Util awsS3Util = new AwsS3Util();
    Optional<BigInteger> customerStorage = Optional.empty();
    if (from == null && to == null) {
      customerStorage = awsS3Util.getS3Storage(customerId);
    } else {
      DateTime fromDate = new DateTime(Instant.EPOCH);
      DateTime toDate = new DateTime(Instant.now());
      if (from != null)
        fromDate = DateTime.parse(from);
      if (to != null)
        toDate = DateTime.parse(to);
      customerStorage = awsS3Util.getS3StorageInInterval(customerId, fromDate, toDate);
    }
    if (customerStorage.isPresent()) {
      System.out
          .println("Storage in S3 for customer: " + customerId + " is:" + customerStorage.get());
    }
  }
}
