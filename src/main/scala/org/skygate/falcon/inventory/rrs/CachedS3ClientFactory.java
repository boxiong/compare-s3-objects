package org.skygate.falcon.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;

/**
 * This CachedS3ClientFactory class provides a serializable wrapper
 * through which to access Amazon S3 instances on Spark workers.
 */
public class CachedS3ClientFactory implements SerializableSupplier<AmazonS3> {
    private transient volatile AmazonS3 s3Client;

    @Override
    public AmazonS3 get() {
        if (s3Client == null) {
            AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClient.builder().build();

            AssumeRoleRequest request = new AssumeRoleRequest()
                .withRoleArn("arn:aws:iam::???:role/???-Role")
                .withDurationSeconds(3600)
                .withRoleSessionName("demo");
            AssumeRoleResult result = stsClient.assumeRole(request);
            Credentials credentials = result.getCredentials();

            BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(),
                credentials.getSessionToken());

            s3Client = AmazonS3Client.builder()
                              .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                              .build();
        }
        return s3Client;
    }
}