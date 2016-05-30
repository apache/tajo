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
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

import static org.apache.http.HttpStatus.SC_OK;

public class MockAmazonS3 implements AmazonS3 {
  private int getObjectHttpCode = SC_OK;
  private int getObjectMetadataHttpCode = SC_OK;

  @Override
  public void setEndpoint(String endpoint) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setRegion(Region region)
    throws IllegalArgumentException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setS3ClientOptions(S3ClientOptions clientOptions) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public ObjectListing listObjects(String bucketName) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public ObjectListing listObjects(String bucketName, String prefix)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
    throws AmazonClientException {
    if (listObjectsRequest.getBucketName().equals("tajo-test") && listObjectsRequest.getPrefix().equals("test/")) {
      MockObjectListing objectListing = new MockObjectListing();
      return objectListing;
    } else {
      throw new TajoInternalError(new UnsupportedException());
    }
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker,
    String delimiter, Integer maxResults) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public Owner getS3AccountOwner()
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public boolean doesBucketExist(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public List<Bucket> listBuckets()
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public String getBucketLocation(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public Bucket createBucket(CreateBucketRequest createBucketRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public Bucket createBucket(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public Bucket createBucket(String bucketName, String region)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketAcl(String bucketName, CannedAccessControlList acl)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucketName, String key) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public S3Object getObject(String bucketName, String key)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public S3Object getObject(GetObjectRequest getObjectRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucket(DeleteBucketRequest deleteBucketRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucket(String bucketName) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, File file) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName,
    String destinationKey) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest copyPartRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteObject(String bucketName, String key) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteVersion(DeleteVersionRequest deleteVersionRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest
    setBucketVersioningConfigurationRequest) throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketLifecycleConfiguration(String bucketName,
                                              BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest
                                                  setBucketLifecycleConfigurationRequest) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketLifecycleConfiguration(String bucketName) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest
                                                     deleteBucketLifecycleConfigurationRequest) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration
    bucketCrossOriginConfiguration) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest
                                                    setBucketCrossOriginConfigurationRequest) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(String bucketName) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest
                                                       deleteBucketCrossOriginConfigurationRequest) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketTaggingConfiguration(String bucketName) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest
                                                   deleteBucketTaggingConfigurationRequest) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest
                                                     setBucketNotificationConfigurationRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration
    bucketNotificationConfiguration)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest
                                                                      getBucketWebsiteConfigurationRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketWebsiteConfiguration(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest
                                                   deleteBucketWebsiteConfigurationRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketPolicy(String bucketName, String policyText)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketPolicy(String bucketName)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public PartListing listParts(ListPartsRequest request)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
    throws AmazonClientException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void restoreObject(RestoreObjectRequest request)
    throws AmazonServiceException {
    throw new TajoInternalError(new UnsupportedException());
  }

  @Override
  public void restoreObject(String bucketName, String key, int expirationInDays)
    throws AmazonServiceException {
    throw new TajoInternalError(new UnsupportedException());
  }
}