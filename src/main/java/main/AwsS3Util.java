package main;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import org.joda.time.DateTime;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AwsS3Util {
  private static final String CASSANDRA = "cassandra/";

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
    String bucketName = "tala-integration-archive";
    List<S3ObjectSummary> objects = s3.listObjectsV2(bucketName).getObjectSummaries();

    DateTime d = new DateTime().minusMonths(6);
    // List<S3ObjectSummary> objects1 = objects.parallelStream()
    // .filter(obj -> obj.getKey().contains("cassandra") &&
    // obj.getKey().contains(customerId)).collect(Collectors.toList());
    // objects1.forEach(ob -> System.out.println(ob.getKey()));
    long storage = objects.parallelStream()
        .filter(
            object -> object.getKey().contains("cassandra") && object.getKey().contains(customerId))
        .mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public Optional<BigInteger> getS3StorageInInterval(String customerId, Integer interval) {
    AmazonS3 s3 = getAwsS3Interface();
    String bucketName = "tala-integration-archive";
    List<S3ObjectSummary> objects = s3.listObjectsV2(bucketName).getObjectSummaries();

    DateTime d = new DateTime().minusMonths(interval);
    long storage = objects.parallelStream().filter(object -> {
      if (object.getKey().contains("cassandra") && object.getKey().contains(customerId)) {
        String date =
            object.getKey().substring(object.getKey().indexOf("cassandra/") + CASSANDRA.length(),
                object.getKey().indexOf(customerId) - 1);
        if (DateTime.parse(date).isAfter(d.toInstant()))
          return true;
      }
      return false;
    }).mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public static void main(String[] args) {
    String customerId = "";
    Integer interval = null;
    if (args != null && args.length > 0) {
      customerId = args[0];
      if (args.length > 1)
        interval = Integer.parseInt(args[1]);
    }
    AwsS3Util awsS3Util = new AwsS3Util();
    Optional<BigInteger> customerStorage = Optional.empty();
    if (interval == null)
      customerStorage = awsS3Util.getS3Storage(customerId);
    else
      customerStorage = awsS3Util.getS3StorageInInterval(customerId, interval);
    if (customerStorage.isPresent()) {
      System.out
          .println("Storage in S3 for customer: " + customerId + " is:" + customerStorage.get());
    }
  }
}
