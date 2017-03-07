package com.github.nachomezzadra.sample;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class S3Test extends BaseSpringTest {

    @Autowired
    private ResourceLoader resourceLoader;

    private String bucketName = "testbucket-australia-ama";

    @Test
    public void shoudlProperlyCreateABucket() {
        AmazonS3 s3client = getAmazonS3();

        try {
            if (!(s3client.doesBucketExist(bucketName))) {
                // Note that CreateBucketRequest does not specify region. So bucket is
                // created in the region specified in the client.
                s3client.createBucket(new CreateBucketRequest(
                        bucketName));
            }
            // Get location.
            String bucketLocation = s3client.getBucketLocation(new GetBucketLocationRequest(bucketName));
            System.out.println("bucket location = " + bucketLocation);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            fail("Error Message: " + ase.getMessage());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            fail("Error Message: " + ace.getMessage());
        }
    }


    private AmazonS3 getAmazonS3() {
        String acessKey = "AKIAM63B4XDOKYR1SASA";
        String secretKey = "HnHpWHNcrCQA3HXVqwCxy/AQ+t4qVJ2MX+oB9Y0z";
        String awsRegion = "us-west-2";
        String endpoint = "https://s3." + awsRegion + ".amazonaws.com";

        BasicAWSCredentials credentials = new BasicAWSCredentials(acessKey,
                secretKey);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(endpoint,
                awsRegion);

        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(endpointConfiguration)
                .disableChunkedEncoding()
                .build();
    }


    @Test
    public void shouldProperlyListAllBuckets() {
        AmazonS3 conn = getAmazonS3();

        List<Bucket> buckets = conn.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName() + "\t" +
                    StringUtils.fromDate(bucket.getCreationDate()));
        }
    }


    @Test
    public void shouldProperlyCreateABucket() {
        AmazonS3 conn = getAmazonS3();
        conn.setEndpoint("http://127.0.0.1:9000");

        CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);
        conn.createBucket(createBucketRequest);
    }


    @Test
    public void shouldProperlyVersionAnExistingBucket() {
        AmazonS3 amazonS3 = getAmazonS3();

        BucketVersioningConfiguration configuration =
                new BucketVersioningConfiguration().withStatus("Enabled");

        SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest =
                new SetBucketVersioningConfigurationRequest(bucketName, configuration);

        amazonS3.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
    }

    @Test
    public void shouldProperlyGetBucketVersioningStatus() {
        AmazonS3 amazonS3 = getAmazonS3();

        BucketVersioningConfiguration conf = amazonS3.getBucketVersioningConfiguration(bucketName);
        System.out.println("bucket versioning configuration status:    " + conf.getStatus());
        assertEquals("Enabled", conf.getStatus());
    }


    @Test
    public void shouldProperlyExistABucket() {
        AmazonS3 amazonS3 = getAmazonS3();

        assertTrue(amazonS3.doesBucketExist("cjet-customer-archive"));
    }


    @Test
    public void shouldProperlyRemoveABucket() {
        AmazonS3 conn = getAmazonS3();
//        conn.setEndpoint("http://127.0.0.1:9000");

        DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
        conn.deleteBucket(deleteBucketRequest);
    }


    @Test
    public void shouldProperlyGetAnObjectFromBucket() {
        AmazonS3 conn = getAmazonS3();

        GetObjectRequest objectRequest = new GetObjectRequest(this.bucketName, "aws-s3-test.properties");

        S3Object object = conn.getObject(objectRequest);
        assertNotNull(object);
        assertNotNull(object.getObjectMetadata().getVersionId());
        System.out.println("Object ID: " + object.getObjectMetadata().getVersionId());
    }

    @Test
    public void shouldProperlyListVersionedObjects() {
        AmazonS3 s3client = getAmazonS3();

        try {
            System.out.println("Listing objects");

            ListVersionsRequest request = new ListVersionsRequest()
                    .withBucketName(bucketName)
                    .withMaxResults(2);
            // you can specify .withPrefix to obtain version list for a specific object or objects with
            // the specified key prefix.

            VersionListing versionListing;
            do {
                versionListing = s3client.listVersions(request);
                for (S3VersionSummary objectSummary :
                        versionListing.getVersionSummaries()) {
                    System.out.println(" - " + objectSummary.getKey() + "  " +
                            "(size = " + objectSummary.getSize() + ")" +
                            "(last modified = " + objectSummary.getLastModified() + ")" +
                            "(versionID = " + objectSummary.getVersionId() + ")");

                }
                request.setKeyMarker(versionListing.getNextKeyMarker());
                request.setVersionIdMarker(versionListing.getNextVersionIdMarker());
            } while (versionListing.isTruncated());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
                    "which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            fail();
        }

    }

    @Test
    public void shouldProperlyUploadAFileToABucket() throws IOException {
        AmazonS3 s3Client = getAmazonS3();
//        s3Client.setEndpoint("http://127.0.0.1:9000");

        File file = resourceLoader.getResource("classpath:aws-s3-test.properties").getFile();
        String keyName = file.getName();

        // Create a list of UploadPartResponse objects. You get one of these
        // for each part upload.
        List<PartETag> partETags = new ArrayList();

        // Step 1: Initialize.
        InitiateMultipartUploadRequest initRequest = new
                InitiateMultipartUploadRequest(bucketName, keyName);

        InitiateMultipartUploadResult initResponse =
                s3Client.initiateMultipartUpload(initRequest);

        long contentLength = file.length();
        long partSize = 5242880; // Set part size to 5 MB.

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 5 MB. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName).withKey(keyName)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // Upload part and add response to our list.
                partETags.add(
                        s3Client.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(
                    bucketName,
                    keyName,
                    initResponse.getUploadId(),
                    partETags);

            s3Client.completeMultipartUpload(compRequest);
        } catch (Exception e) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                    bucketName, keyName, initResponse.getUploadId()));
            fail(e.getMessage());
        }
    }


}
