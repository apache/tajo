/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import static org.apache.http.HttpStatus.SC_OK;

public class MockAmazonS3 implements AmazonS3 {
  private int getObjectHttpCode = SC_OK;
  private int getObjectMetadataHttpCode = SC_OK;

  public void setGetObjectHttpErrorCode(int getObjectHttpErrorCode) {
    this.getObjectHttpCode = getObjectHttpErrorCode;
  }

  public void setGetObjectMetadataHttpCode(int getObjectMetadataHttpCode) {
    this.getObjectMetadataHttpCode = getObjectMetadataHttpCode;
  }

  @Override
  public void setEndpoint(String endpoint) {
  }

  @Override
  public void setRegion(Region region)
    throws IllegalArgumentException {
  }

  @Override
  public void setS3ClientOptions(S3ClientOptions clientOptions) {
  }

  @Override
  public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass)
    throws AmazonClientException {
  }

  @Override
  public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
    throws AmazonClientException {
  }

  @Override
  public ObjectListing listObjects(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public ObjectListing listObjects(String bucketName, String prefix)
    throws AmazonClientException {
    return null;
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
    throws AmazonClientException {
    if (listObjectsRequest.getBucketName().equals("tajo-test") && listObjectsRequest.getPrefix().equals("test/")) {
      MockObjectListing objectListing = new MockObjectListing();
      return objectListing;
    } else {
      return null;
    }
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
    throws AmazonClientException {
    return null;
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix)
    throws AmazonClientException {
    return null;
  }

  @Override
  public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
    throws AmazonClientException {
    return null;
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker,
    String delimiter, Integer maxResults) throws AmazonClientException {
    return null;
  }

  @Override
  public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public Owner getS3AccountOwner()
    throws AmazonClientException {
    return null;
  }

  @Override
  public boolean doesBucketExist(String bucketName)
    throws AmazonClientException {
    return false;
  }

  @Override
  public List<Bucket> listBuckets()
    throws AmazonClientException {
    return null;
  }

  @Override
  public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public String getBucketLocation(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public Bucket createBucket(CreateBucketRequest createBucketRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public Bucket createBucket(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
    throws AmazonClientException {
    return null;
  }

  @Override
  public Bucket createBucket(String bucketName, String region)
    throws AmazonClientException {
    return null;
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key)
    throws AmazonClientException {
    return null;
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl)
    throws AmazonClientException {
  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
    throws AmazonClientException {
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
    throws AmazonClientException {
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl)
    throws AmazonClientException {
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
    throws AmazonClientException {
  }

  @Override
  public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl)
    throws AmazonClientException {
  }

  @Override
  public void setBucketAcl(String bucketName, CannedAccessControlList acl)
    throws AmazonClientException {
  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucketName, String key)
    throws AmazonClientException {
    if (getObjectMetadataHttpCode != SC_OK) {
      AmazonS3Exception exception = new AmazonS3Exception("Failing getObjectMetadata call with "
        + getObjectMetadataHttpCode);
      exception.setStatusCode(getObjectMetadataHttpCode);
      throw exception;
    }
    return null;
  }

  @Override
  public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public S3Object getObject(String bucketName, String key)
    throws AmazonClientException {
    return null;
  }

  @Override
  public S3Object getObject(GetObjectRequest getObjectRequest)
    throws AmazonClientException {
    if (getObjectHttpCode != SC_OK) {
      AmazonS3Exception exception = new AmazonS3Exception("Failing getObject call with " + getObjectHttpCode);
      exception.setStatusCode(getObjectHttpCode);
      throw exception;
    }
    return null;
  }

  @Override
  public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
    throws AmazonClientException {
  }

  @Override
  public void deleteBucket(String bucketName)
    throws AmazonClientException {
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, File file)
    throws AmazonClientException {
    return null;
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
    throws AmazonClientException {
    return null;
  }

  @Override
  public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName,
    String destinationKey) throws AmazonClientException {
    return null;
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest copyPartRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void deleteObject(String bucketName, String key)
    throws AmazonClientException {
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest)
    throws AmazonClientException {
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId)
    throws AmazonClientException {
  }

  @Override
  public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
    throws AmazonClientException {
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
    throws AmazonClientException {
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest
    setBucketVersioningConfigurationRequest) throws AmazonClientException {
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    return null;
  }

  @Override
  public void setBucketLifecycleConfiguration(String bucketName,
                                              BucketLifecycleConfiguration bucketLifecycleConfiguration) {
  }

  @Override
  public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest
                                                  setBucketLifecycleConfigurationRequest) {
  }

  @Override
  public void deleteBucketLifecycleConfiguration(String bucketName) {
  }

  @Override
  public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest
                                                     deleteBucketLifecycleConfigurationRequest) {
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
    return null;
  }

  @Override
  public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration
    bucketCrossOriginConfiguration) {
  }

  @Override
  public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest
                                                    setBucketCrossOriginConfigurationRequest) {
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(String bucketName) {
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest
                                                       deleteBucketCrossOriginConfigurationRequest) {
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
    return null;
  }

  @Override
  public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
  }

  @Override
  public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
  }

  @Override
  public void deleteBucketTaggingConfiguration(String bucketName) {
  }

  @Override
  public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest
                                                   deleteBucketTaggingConfigurationRequest) {
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest
                                                     setBucketNotificationConfigurationRequest)
    throws AmazonClientException {
  }

  @Override
  public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration
    bucketNotificationConfiguration)
    throws AmazonClientException {
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest
                                                                      getBucketWebsiteConfigurationRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration)
    throws AmazonClientException {
  }

  @Override
  public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
    throws AmazonClientException {
  }

  @Override
  public void deleteBucketWebsiteConfiguration(String bucketName)
    throws AmazonClientException {
  }

  @Override
  public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest
                                                   deleteBucketWebsiteConfigurationRequest)
    throws AmazonClientException {
  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName)
    throws AmazonClientException {
    return null;
  }

  @Override
  public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void setBucketPolicy(String bucketName, String policyText)
    throws AmazonClientException {
  }

  @Override
  public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
    throws AmazonClientException {
  }

  @Override
  public void deleteBucketPolicy(String bucketName)
    throws AmazonClientException {
  }

  @Override
  public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
    throws AmazonClientException {
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration)
    throws AmazonClientException {
    return null;
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
    throws AmazonClientException {
    return null;
  }

  @Override
  public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest)
    throws AmazonClientException {
    return null;
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
    throws AmazonClientException {
    return null;
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request)
    throws AmazonClientException {
    return null;
  }

  @Override
  public PartListing listParts(ListPartsRequest request)
    throws AmazonClientException {
    return null;
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request)
    throws AmazonClientException {
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
    throws AmazonClientException {
    return null;
  }

  @Override
  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
    throws AmazonClientException {
    return null;
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    return null;
  }

  @Override
  public void restoreObject(RestoreObjectRequest request)
    throws AmazonServiceException {
  }

  @Override
  public void restoreObject(String bucketName, String key, int expirationInDays)
    throws AmazonServiceException {
  }

}