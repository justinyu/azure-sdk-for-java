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
package com.microsoft.windowsazure.services.queue.client;

import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.windowsazure.services.core.storage.StorageException;

/**
 * Table Client Tests
 */
public class CloudQueueTests extends QueueTestBase {
    @Test
    public void queueGetSetPermissionTest() throws StorageException, URISyntaxException {
        String name = this.generateRandomQueueName();
        CloudQueue newQueue = qClient.getQueueReference(name);
        newQueue.create();

        QueuePermissions expectedPermissions;
        QueuePermissions testPermissions;

        try {
            // Test new permissions.
            expectedPermissions = new QueuePermissions();
            testPermissions = newQueue.downloadPermissions();
            assertQueuePermissionsEqual(expectedPermissions, testPermissions);

            // Test setting empty permissions.
            newQueue.uploadPermissions(expectedPermissions);
            testPermissions = newQueue.downloadPermissions();
            assertQueuePermissionsEqual(expectedPermissions, testPermissions);

            // Add a policy, check setting and getting.
            SharedAccessQueuePolicy policy1 = new SharedAccessQueuePolicy();
            Calendar now = GregorianCalendar.getInstance();
            policy1.setSharedAccessStartTime(now.getTime());
            now.add(Calendar.MINUTE, 10);
            policy1.setSharedAccessExpiryTime(now.getTime());

            policy1.setPermissions(EnumSet.of(SharedAccessQueuePermissions.READ,
                    SharedAccessQueuePermissions.PROCESSMESSAGES, SharedAccessQueuePermissions.ADD,
                    SharedAccessQueuePermissions.UPDATE));
            expectedPermissions.getSharedAccessPolicies().put(UUID.randomUUID().toString(), policy1);

            newQueue.uploadPermissions(expectedPermissions);
            testPermissions = newQueue.downloadPermissions();
            assertQueuePermissionsEqual(expectedPermissions, testPermissions);
        }
        finally {
            // cleanup
            newQueue.deleteIfExists();
        }
    }

    static void assertQueuePermissionsEqual(QueuePermissions expected, QueuePermissions actual) {
        HashMap<String, SharedAccessQueuePolicy> expectedPolicies = expected.getSharedAccessPolicies();
        HashMap<String, SharedAccessQueuePolicy> actualPolicies = actual.getSharedAccessPolicies();
        Assert.assertEquals("SharedAccessPolicies.Count", expectedPolicies.size(), actualPolicies.size());
        for (String name : expectedPolicies.keySet()) {
            Assert.assertTrue("Key" + name + " doesn't exist", actualPolicies.containsKey(name));
            SharedAccessQueuePolicy expectedPolicy = expectedPolicies.get(name);
            SharedAccessQueuePolicy actualPolicy = actualPolicies.get(name);
            Assert.assertEquals("Policy: " + name + "\tPermissions\n", expectedPolicy.getPermissions().toString(),
                    actualPolicy.getPermissions().toString());
            Assert.assertEquals("Policy: " + name + "\tStartDate\n", expectedPolicy.getSharedAccessStartTime()
                    .toString(), actualPolicy.getSharedAccessStartTime().toString());
            Assert.assertEquals("Policy: " + name + "\tExpireDate\n", expectedPolicy.getSharedAccessExpiryTime()
                    .toString(), actualPolicy.getSharedAccessExpiryTime().toString());

        }

    }
}
