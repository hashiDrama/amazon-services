package main;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
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
    ObjectListing ob = s3.listObjects(bucketName);
    List<S3ObjectSummary> objects = ob.getObjectSummaries();
    while (ob.isTruncated()) {
      ob = s3.listNextBatchOfObjects(ob);
      objects.addAll(ob.getObjectSummaries());
    }
    List<S3ObjectSummary> filteredData = objects.stream()
        .filter(
            object -> object.getKey().contains("cassandra") && object.getKey().contains(customerId))
        .collect(Collectors.toList());
    long storage = filteredData.parallelStream().mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public Optional<BigInteger> getS3StorageInInterval(String customerId, DateTime from,
      DateTime to) {
    AmazonS3 s3 = getAwsS3Interface();
    String bucketName = "tala-" + env + "-archive";
    ObjectListing ob = s3.listObjects(bucketName);
    List<S3ObjectSummary> objects = ob.getObjectSummaries();
    while (ob.isTruncated()) {
      ob = s3.listNextBatchOfObjects(ob);
      objects.addAll(ob.getObjectSummaries());
    }
    List<S3ObjectSummary> filteredData = objects.parallelStream().filter(object -> {
      if (object.getKey().contains("cassandra") && object.getKey().contains(customerId)) {
        DateTime date = DateTime.parse(
            object.getKey().substring(object.getKey().indexOf("cassandra/") + CASSANDRA.length(),
                object.getKey().indexOf(customerId) - 1));
        if ((date.isEqual(from.toInstant()) || date.isAfter(from.toInstant()))
            && (date.isEqual(to.toInstant()) || date.isBefore(to.toInstant())))
          return true;
      }
      return false;
    }).collect(Collectors.toList());
    long storage = filteredData.stream().mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public static void main(String[] args) {
    ArgumentParser parser =
        ArgumentParsers.newArgumentParser("s3-storage").defaultHelp(true).description("S3 storage");
    parser.addArgument("--env")
        .help("Specify the environment.Has to be one of [ integration, staging, production ]")
        .type(String.class).setDefault("staging");

    parser.addArgument("--customer.id").help("Customer id").type(String.class).required(true);

    parser.addArgument("--from")
        .help("Specify the date after which records are needed. Format should be -> yyyy-mm-dd")
        .type(String.class).required(false);

    parser.addArgument("--to")
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
    String from = ns.getString("from");
    String to = ns.getString("to");

    AwsS3Util awsS3Util = new AwsS3Util();
    Optional<BigInteger> customerStorage = Optional.empty();
    if (from == null && to == null) {
      customerStorage = awsS3Util.getS3Storage(customerId);
    } else {
      DateTime fromDate = new DateTime(0, DateTimeZone.UTC);
      DateTime toDate = new DateTime(DateTimeZone.UTC);
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
