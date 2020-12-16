import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AwsS3Util {

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

    long storage = objects.parallelStream()
        .filter(
            object -> object.getKey().contains("cassandra") && object.getKey().contains(customerId))
        .mapToLong(S3ObjectSummary::getSize).sum();
    return Optional.ofNullable(BigInteger.valueOf(storage));
  }

  public static void main(String[] args) {
    String customerId = "";
    if (args != null && args.length > 0) {
      customerId = args[0];
    }
    AwsS3Util awsS3Util = new AwsS3Util();
    Optional<BigInteger> customerStorage = awsS3Util.getS3Storage(customerId);
    if (customerStorage.isPresent()) {
      System.out
          .println("Storage in S3 for customer: " + customerId + " is:" + customerStorage.get());
    }
  }
}
