/**
 * Copyright 2011 Microsoft Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.windowsazure.services.blob.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.windowsazure.services.core.storage.AccessCondition;
import com.microsoft.windowsazure.services.core.storage.OperationContext;
import com.microsoft.windowsazure.services.core.storage.StorageErrorCodeStrings;
import com.microsoft.windowsazure.services.core.storage.StorageException;

/**
 * Table Client Tests
 */
public class CloudBlobContainerTests extends BlobTestBase {
    @Test
    public void testContainerGetSetPermission() throws StorageException, URISyntaxException {
        String name = this.generateRandomContainerName();
        CloudBlobContainer newContainer = bClient.getContainerReference(name);
        newContainer.create();

        BlobContainerPermissions expectedPermissions;
        BlobContainerPermissions testPermissions;

        try {
            // Test new permissions.
            expectedPermissions = new BlobContainerPermissions();
            testPermissions = newContainer.downloadPermissions();
            assertTablePermissionsEqual(expectedPermissions, testPermissions);

            // Test setting empty permissions.
            newContainer.uploadPermissions(expectedPermissions);
            testPermissions = newContainer.downloadPermissions();
            assertTablePermissionsEqual(expectedPermissions, testPermissions);

            // Add a policy, check setting and getting.
            SharedAccessBlobPolicy policy1 = new SharedAccessBlobPolicy();
            Calendar now = GregorianCalendar.getInstance();
            policy1.setSharedAccessStartTime(now.getTime());
            now.add(Calendar.MINUTE, 10);
            policy1.setSharedAccessExpiryTime(now.getTime());

            policy1.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ, SharedAccessBlobPermissions.DELETE,
                    SharedAccessBlobPermissions.LIST, SharedAccessBlobPermissions.DELETE));
            expectedPermissions.getSharedAccessPolicies().put(UUID.randomUUID().toString(), policy1);

            newContainer.uploadPermissions(expectedPermissions);
            testPermissions = newContainer.downloadPermissions();
            assertTablePermissionsEqual(expectedPermissions, testPermissions);
        }
        finally {
            // cleanup
            newContainer.deleteIfExists();
        }
    }

    static void assertTablePermissionsEqual(BlobContainerPermissions expected, BlobContainerPermissions actual) {
        HashMap<String, SharedAccessBlobPolicy> expectedPolicies = expected.getSharedAccessPolicies();
        HashMap<String, SharedAccessBlobPolicy> actualPolicies = actual.getSharedAccessPolicies();
        Assert.assertEquals("SharedAccessPolicies.Count", expectedPolicies.size(), actualPolicies.size());
        for (String name : expectedPolicies.keySet()) {
            Assert.assertTrue("Key" + name + " doesn't exist", actualPolicies.containsKey(name));
            SharedAccessBlobPolicy expectedPolicy = expectedPolicies.get(name);
            SharedAccessBlobPolicy actualPolicy = actualPolicies.get(name);
            Assert.assertEquals("Policy: " + name + "\tPermissions\n", expectedPolicy.getPermissions().toString(),
                    actualPolicy.getPermissions().toString());
            Assert.assertEquals("Policy: " + name + "\tStartDate\n", expectedPolicy.getSharedAccessStartTime()
                    .toString(), actualPolicy.getSharedAccessStartTime().toString());
            Assert.assertEquals("Policy: " + name + "\tExpireDate\n", expectedPolicy.getSharedAccessExpiryTime()
                    .toString(), actualPolicy.getSharedAccessExpiryTime().toString());

        }

    }

    @Test
    public void testContainerAcquireLease() throws StorageException, URISyntaxException, InterruptedException {
        String name = "leased" + this.generateRandomContainerName();
        CloudBlobContainer leaseContainer1 = bClient.getContainerReference(name);
        leaseContainer1.create();
        String proposedLeaseId1 = UUID.randomUUID().toString();

        name = "leased" + this.generateRandomContainerName();
        CloudBlobContainer leaseContainer2 = bClient.getContainerReference(name);
        leaseContainer2.create();
        String proposedLeaseId2 = UUID.randomUUID().toString();

        try {
            // 15 sec

            OperationContext operationContext1 = new OperationContext();
            leaseContainer1.acquireLease(15, proposedLeaseId1, null /*access condition*/,
                    null/* BlobRequestOptions */, operationContext1);
            Assert.assertTrue(operationContext1.getLastResult().getStatusCode() == HttpURLConnection.HTTP_CREATED);

            //infinite
            String leaseId1;
            String leaseId2;
            OperationContext operationContext2 = new OperationContext();
            leaseId1 = leaseContainer2.acquireLease(null /* infinite lease */, proposedLeaseId2,
                    null /*access condition*/, null/* BlobRequestOptions */, operationContext2);
            Assert.assertTrue(operationContext2.getLastResult().getStatusCode() == HttpURLConnection.HTTP_CREATED);

            leaseId2 = leaseContainer2.acquireLease(null /* infinite lease */, proposedLeaseId2);
            Assert.assertEquals(leaseId1, leaseId2);

        }
        finally {
            // cleanup
            AccessCondition condition = new AccessCondition();
            condition.setLeaseID(proposedLeaseId1);
            leaseContainer1.releaseLease(condition);
            leaseContainer1.deleteIfExists();

            condition = new AccessCondition();
            condition.setLeaseID(proposedLeaseId2);
            leaseContainer2.releaseLease(condition);
            leaseContainer2.deleteIfExists();
        }
    }

    @Test
    public void testContainerReleaseLease() throws StorageException, URISyntaxException, InterruptedException {
        String name = "leased" + this.generateRandomContainerName();
        CloudBlobContainer newContainer = bClient.getContainerReference(name);
        newContainer.create();

        try {
            // 15 sec
            String proposedLeaseId = UUID.randomUUID().toString();
            String leaseId = newContainer.acquireLease(15, proposedLeaseId);
            AccessCondition condition = new AccessCondition();
            condition.setLeaseID(leaseId);
            OperationContext operationContext1 = new OperationContext();
            newContainer.releaseLease(condition, null/* BlobRequestOptions */, operationContext1);
            Assert.assertTrue(operationContext1.getLastResult().getStatusCode() == HttpURLConnection.HTTP_OK);

            //infinite
            proposedLeaseId = UUID.randomUUID().toString();
            leaseId = newContainer.acquireLease(null /* infinite lease */, proposedLeaseId);
            condition = new AccessCondition();
            condition.setLeaseID(leaseId);
            OperationContext operationContext2 = new OperationContext();
            newContainer.releaseLease(condition, null/* BlobRequestOptions */, operationContext2);
            Assert.assertTrue(operationContext2.getLastResult().getStatusCode() == HttpURLConnection.HTTP_OK);
        }
        finally {
            // cleanup
            newContainer.deleteIfExists();
        }
    }

    @Test
    public void testContainerBreakLease() throws StorageException, URISyntaxException, InterruptedException {
        String name = "leased" + this.generateRandomContainerName();
        CloudBlobContainer newContainer = bClient.getContainerReference(name);
        newContainer.create();
        String proposedLeaseId = UUID.randomUUID().toString();

        try {
            // 5 sec
            String leaseId = newContainer.acquireLease(5, proposedLeaseId);
            AccessCondition condition = new AccessCondition();
            condition.setLeaseID(leaseId);
            OperationContext operationContext1 = new OperationContext();
            newContainer.breakLease(0, condition, null/* BlobRequestOptions */, operationContext1);
            Assert.assertTrue(operationContext1.getLastResult().getStatusCode() == HttpURLConnection.HTTP_ACCEPTED);
            Thread.sleep(5 * 1000);

            //infinite
            proposedLeaseId = UUID.randomUUID().toString();
            leaseId = newContainer.acquireLease(null /* infinite lease */, proposedLeaseId);
            condition = new AccessCondition();
            condition.setLeaseID(leaseId);
            OperationContext operationContext2 = new OperationContext();
            newContainer.breakLease(0, condition, null/* BlobRequestOptions */, operationContext2);
            Assert.assertTrue(operationContext2.getLastResult().getStatusCode() == HttpURLConnection.HTTP_ACCEPTED);
        }
        finally {
            // cleanup
            AccessCondition condition = new AccessCondition();
            condition.setLeaseID(proposedLeaseId);
            newContainer.releaseLease(condition);
            newContainer.deleteIfExists();
        }
    }

    @Test
    public void testContainerRenewLeaseTest() throws StorageException, URISyntaxException, InterruptedException {
        String name = "leased" + this.generateRandomContainerName();
        CloudBlobContainer newContainer = bClient.getContainerReference(name);
        newContainer.create();
        String proposedLeaseId = UUID.randomUUID().toString();

        try {
            // 5 sec
            String leaseId = newContainer.acquireLease(5, proposedLeaseId);
            AccessCondition condition = new AccessCondition();
            condition.setLeaseID(leaseId);
            OperationContext operationContext1 = new OperationContext();
            newContainer.renewLease(condition, null/* BlobRequestOptions */, operationContext1);
            Assert.assertTrue(operationContext1.getLastResult().getStatusCode() == HttpURLConnection.HTTP_OK);
            newContainer.releaseLease(condition);

            //infinite
            proposedLeaseId = UUID.randomUUID().toString();
            leaseId = newContainer.acquireLease(null /* infinite lease */, proposedLeaseId);
            condition = new AccessCondition();
            condition.setLeaseID(leaseId);
            OperationContext operationContext2 = new OperationContext();
            newContainer.renewLease(condition, null/* BlobRequestOptions */, operationContext2);
            Assert.assertTrue(operationContext2.getLastResult().getStatusCode() == HttpURLConnection.HTTP_OK);
            newContainer.releaseLease(condition);
        }
        finally {
            // cleanup
            AccessCondition condition = new AccessCondition();
            condition.setLeaseID(proposedLeaseId);
            newContainer.releaseLease(condition);
            newContainer.deleteIfExists();
        }
    }

    @Test
    public void testBlobLeaseAcquireAndRelease() throws URISyntaxException, StorageException, IOException {
        final int length = 128;
        final Random randGenerator = new Random();
        final byte[] buff = new byte[length];
        randGenerator.nextBytes(buff);

        String blobName = "testBlob" + Integer.toString(randGenerator.nextInt(50000));
        blobName = blobName.replace('-', '_');

        final CloudBlobContainer leasedContainer = bClient.getContainerReference(this.testSuiteContainerName);
        final CloudBlob blobRef = leasedContainer.getBlockBlobReference(blobName);
        final BlobRequestOptions options = new BlobRequestOptions();

        blobRef.upload(new ByteArrayInputStream(buff), -1, null, options, null);

        // Get Lease 
        OperationContext operationContext = new OperationContext();
        final String leaseID = blobRef.acquireLease(15, null /*access condition*/, null /*proposed lease id */,
                null/* BlobRequestOptions */, operationContext);
        final AccessCondition leaseCondition = AccessCondition.generateLeaseCondition(leaseID);
        Assert.assertTrue(operationContext.getLastResult().getStatusCode() == HttpURLConnection.HTTP_CREATED);

        // Try to upload without lease
        try {
            blobRef.upload(new ByteArrayInputStream(buff), -1, null, options, null);
        }
        catch (final StorageException ex) {
            Assert.assertEquals(ex.getHttpStatusCode(), 412);
            Assert.assertEquals(ex.getErrorCode(), StorageErrorCodeStrings.LEASE_ID_MISSING);
        }

        // Try to upload with lease
        blobRef.upload(new ByteArrayInputStream(buff), -1, leaseCondition, options, null);

        // Release lease
        blobRef.releaseLease(leaseCondition);

        // now upload with no lease specified.
        blobRef.upload(new ByteArrayInputStream(buff), -1, null, options, null);
    }

    @Test
    public void testBlobLeaseBreak() throws URISyntaxException, StorageException, IOException, InterruptedException {
        final int length = 128;
        final Random randGenerator = new Random();
        final byte[] buff = new byte[length];
        randGenerator.nextBytes(buff);

        String blobName = "testBlob" + Integer.toString(randGenerator.nextInt(50000));
        blobName = blobName.replace('-', '_');

        final CloudBlobContainer existingContainer = bClient.getContainerReference(this.testSuiteContainerName);
        final CloudBlob blobRef = existingContainer.getBlockBlobReference(blobName);
        final BlobRequestOptions options = new BlobRequestOptions();

        blobRef.upload(new ByteArrayInputStream(buff), -1, null, options, null);

        // Get Lease
        String leaseID = blobRef.acquireLease(null, null);

        OperationContext operationContext = new OperationContext();
        final AccessCondition leaseCondition = AccessCondition.generateLeaseCondition(leaseID);
        blobRef.breakLease(0, leaseCondition, null/* BlobRequestOptions */, operationContext);
        Assert.assertTrue(operationContext.getLastResult().getStatusCode() == HttpURLConnection.HTTP_ACCEPTED);
    }

    @Test
    public void testBlobLeaseRenew() throws URISyntaxException, StorageException, IOException, InterruptedException {
        final int length = 128;
        final Random randGenerator = new Random();
        final byte[] buff = new byte[length];
        randGenerator.nextBytes(buff);

        String blobName = "testBlob" + Integer.toString(randGenerator.nextInt(50000));
        blobName = blobName.replace('-', '_');

        final CloudBlobContainer existingContainer = bClient.getContainerReference(this.testSuiteContainerName);
        final CloudBlob blobRef = existingContainer.getBlockBlobReference(blobName);
        final BlobRequestOptions options = new BlobRequestOptions();

        blobRef.upload(new ByteArrayInputStream(buff), -1, null, options, null);

        // Get Lease
        final String leaseID = blobRef.acquireLease(15, null);
        Thread.sleep(1000);

        AccessCondition leaseCondition = AccessCondition.generateLeaseCondition(leaseID);
        OperationContext operationContext = new OperationContext();
        blobRef.renewLease(leaseCondition, null/* BlobRequestOptions */, operationContext);
        Assert.assertTrue(operationContext.getLastResult().getStatusCode() == HttpURLConnection.HTTP_OK);
    }

    static String setLeasedState(CloudBlobContainer container, int leaseTime) throws StorageException {
        String leaseId = UUID.randomUUID().toString();
        setUnleasedState(container);
        return container.acquireLease(leaseTime, leaseId);
    }

    static void setUnleasedState(CloudBlobContainer container) throws StorageException {
        if (!container.createIfNotExist()) {
            try {
                container.breakLease(0);
            }
            catch (StorageException e) {
                if (e.getHttpStatusCode() != HttpURLConnection.HTTP_BAD_REQUEST) {
                    throw e;
                }
            }
        }
    }

    @Test
    public void testCopyFromBlob() throws StorageException, URISyntaxException, IOException, InterruptedException {
        String name = this.generateRandomContainerName();
        CloudBlobContainer newContainer = bClient.getContainerReference(name);
        newContainer.create();
        CloudBlob newBlob = newContainer.getBlockBlobReference("newblob");
        newBlob.upload(new ByteArrayInputStream(testData), testData.length);

        try {
            CloudBlob copyBlob = newContainer.getBlockBlobReference(newBlob.getName() + "copyed");
            copyBlob.copyFromBlob(newBlob);
            Thread.sleep(1000);
            copyBlob.downloadAttributes();
            Assert.assertNotNull(copyBlob.copyState);
            Assert.assertNotNull(copyBlob.copyState.getCopyId());
            Assert.assertNotNull(copyBlob.copyState.getCompletionTime());
            Assert.assertNotNull(copyBlob.copyState.getSource());
            Assert.assertFalse(copyBlob.copyState.getBytesCopied() == 0);
            Assert.assertFalse(copyBlob.copyState.getTotalBytes() == 0);
            for (final ListBlobItem blob : newContainer.listBlobs()) {
                CloudBlob blobFromList = ((CloudBlob) blob);
                blobFromList.downloadAttributes();
            }
        }
        finally {
            // cleanup
            newContainer.deleteIfExists();
        }
    }

    @Test
    public void testCopyFromBlobWaitUntilCompleteTest() throws StorageException, URISyntaxException, IOException,
            InterruptedException {
        String name = this.generateRandomContainerName();
        CloudBlobContainer newContainer = bClient.getContainerReference(name);
        newContainer.create();
        CloudBlob newBlob = newContainer.getBlockBlobReference("newblob");
        newBlob.upload(new ByteArrayInputStream(testData), testData.length);

        try {
            CloudBlob copyBlob = newContainer.getBlockBlobReference(newBlob.getName() + "copyed");
            copyBlob.copyFromBlobWaitUntilComplete(newBlob, 2, 10, null, null, null, null);
            Thread.sleep(1000);
            copyBlob.downloadAttributes();
            Assert.assertNotNull(copyBlob.copyState);
            Assert.assertNotNull(copyBlob.copyState.getCopyId());
            Assert.assertNotNull(copyBlob.copyState.getCompletionTime());
            Assert.assertNotNull(copyBlob.copyState.getSource());
            Assert.assertFalse(copyBlob.copyState.getBytesCopied() == 0);
            Assert.assertFalse(copyBlob.copyState.getTotalBytes() == 0);
            for (final ListBlobItem blob : newContainer.listBlobs()) {
                CloudBlob blobFromList = ((CloudBlob) blob);
                blobFromList.downloadAttributes();
            }
        }
        finally {
            // cleanup
            newContainer.deleteIfExists();
        }
    }
}
