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
package com.microsoft.windowsazure.services.table.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import com.microsoft.windowsazure.services.core.storage.ResultSegment;
import com.microsoft.windowsazure.services.core.storage.StorageException;

/**
 * Table Client Tests
 */
public class TableClientTests extends TableTestBase {
    @Test
    public void listTablesSegmented() throws IOException, URISyntaxException, StorageException {
        String tableBaseName = generateRandomTableName();

        ArrayList<String> tables = new ArrayList<String>();
        for (int m = 0; m < 20; m++) {
            String name = String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(m));
            CloudTable table = tClient.getTableReference(name);
            table.create();
            tables.add(name);
        }

        try {
            int currTable = 0;
            ResultSegment<String> segment1 = tClient.listTablesSegmented(tableBaseName, 5, null, null, null);
            Assert.assertEquals(5, segment1.getLength());
            for (String s : segment1.getResults()) {
                Assert.assertEquals(s,
                        String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                currTable++;
            }

            ResultSegment<String> segment2 = tClient.listTablesSegmented(tableBaseName, 5,
                    segment1.getContinuationToken(), null, null);
            Assert.assertEquals(5, segment2.getLength());
            for (String s : segment2.getResults()) {
                Assert.assertEquals(s,
                        String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                currTable++;
            }

            ResultSegment<String> segment3 = tClient.listTablesSegmented(tableBaseName, 5,
                    segment2.getContinuationToken(), null, null);
            Assert.assertEquals(5, segment3.getLength());
            for (String s : segment3.getResults()) {
                Assert.assertEquals(s,
                        String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                currTable++;
            }
        }
        finally {
            for (String s : tables) {
                CloudTable table = tClient.getTableReference(s);
                table.delete();
            }
        }
    }

    @Test
    public void listTablesSegmentedNoPrefix() throws IOException, URISyntaxException, StorageException {
        String tableBaseName = generateRandomTableName();
        ArrayList<String> tables = new ArrayList<String>();
        for (int m = 0; m < 20; m++) {
            String name = String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(m));
            CloudTable table = tClient.getTableReference(name);
            table.create();
            tables.add(name);
        }

        try {
            int currTable = 0;
            ResultSegment<String> segment1 = tClient.listTablesSegmented(null, 5, null, null, null);
            Assert.assertEquals(5, segment1.getLength());
            for (String s : segment1.getResults()) {
                if (s.startsWith(tableBaseName)) {
                    Assert.assertEquals(s,
                            String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                    currTable++;
                }
            }

            ResultSegment<String> segment2 = tClient.listTablesSegmented(null, 5, segment1.getContinuationToken(),
                    null, null);
            Assert.assertEquals(5, segment2.getLength());
            for (String s : segment2.getResults()) {
                if (s.startsWith(tableBaseName)) {
                    Assert.assertEquals(s,
                            String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                    currTable++;
                }
            }

            ResultSegment<String> segment3 = tClient.listTablesSegmented(null, 5, segment2.getContinuationToken(),
                    null, null);
            Assert.assertEquals(5, segment3.getLength());
            for (String s : segment3.getResults()) {
                if (s.startsWith(tableBaseName)) {
                    Assert.assertEquals(s,
                            String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                    currTable++;
                }

            }
        }
        finally {
            for (String s : tables) {
                CloudTable table = tClient.getTableReference(s);
                table.delete();
            }
        }
    }

    @Test
    public void listTablesWithIterator() throws IOException, URISyntaxException, StorageException {
        String tableBaseName = generateRandomTableName();
        ArrayList<String> tables = new ArrayList<String>();
        for (int m = 0; m < 20; m++) {
            String name = String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(m));
            CloudTable table = tClient.getTableReference(name);
            table.create();
            tables.add(name);
        }

        try {
            // With prefix
            int currTable = 0;
            for (String s : tClient.listTables(tableBaseName, null, null)) {
                Assert.assertEquals(s,
                        String.format("%s%s", tableBaseName, new DecimalFormat("#0000").format(currTable)));
                currTable++;
            }

            Assert.assertEquals(20, currTable);

            // Without prefix
            currTable = 0;
            for (String s : tClient.listTables()) {
                if (s.startsWith(tableBaseName)) {
                    currTable++;
                }
            }

            Assert.assertEquals(20, currTable);
        }
        finally {
            for (String s : tables) {
                CloudTable table = tClient.getTableReference(s);
                table.delete();
            }
        }
    }

    @Test
    public void tableCreateAndAttemptCreateOnceExists() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);
        try {
            table.create();
            Assert.assertTrue(table.exists());

            // Should fail as it already exists
            try {
                table.create();
                fail();
            }
            catch (StorageException ex) {
                Assert.assertEquals(ex.getErrorCode(), "TableAlreadyExists");
            }
        }
        finally {
            // cleanup
            table.deleteIfExists();
        }
    }

    @Test
    public void tableCreateExistsAndDelete() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);
        try {
            Assert.assertTrue(table.createIfNotExist());
            Assert.assertTrue(table.exists());
            Assert.assertTrue(table.deleteIfExists());
        }
        finally {
            // cleanup
            table.deleteIfExists();
        }
    }

    @Test
    public void tableCreateIfNotExists() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);
        try {
            Assert.assertTrue(table.createIfNotExist());
            Assert.assertTrue(table.exists());
            Assert.assertFalse(table.createIfNotExist());
        }
        finally {
            // cleanup
            table.deleteIfExists();
        }
    }

    @Test
    public void tableDeleteIfExists() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);

        Assert.assertFalse(table.deleteIfExists());

        table.create();
        Assert.assertTrue(table.exists());
        Assert.assertTrue(table.deleteIfExists());
        Assert.assertFalse(table.deleteIfExists());
    }

    @Test
    public void tableDeleteWhenExistAndNotExists() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);

        try {
            // Should fail as it doesnt already exists
            try {
                table.delete();
                fail();
            }
            catch (StorageException ex) {
                Assert.assertEquals(ex.getMessage(), "Not Found");
            }

            table.create();
            Assert.assertTrue(table.exists());
            table.delete();
            Assert.assertFalse(table.exists());
        }
        finally {
            table.deleteIfExists();
        }
    }

    @Test
    public void tableDoesTableExist() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);

        try {
            Assert.assertFalse(table.exists());
            Assert.assertTrue(table.createIfNotExist());
            Assert.assertTrue(table.exists());
        }
        finally {
            // cleanup
            table.deleteIfExists();
        }
    }

    @Test
    public void tableGetSetPermissionTest() throws StorageException, URISyntaxException {
        String tableName = generateRandomTableName();
        CloudTable table = tClient.getTableReference(tableName);
        table.create();

        TablePermissions expectedPermissions;
        TablePermissions testPermissions;

        try {
            // Test new permissions.
            expectedPermissions = new TablePermissions();
            testPermissions = table.downloadPermissions();
            assertTablePermissionsEqual(expectedPermissions, testPermissions);

            // Test setting empty permissions.
            table.uploadPermissions(expectedPermissions);
            testPermissions = table.downloadPermissions();
            assertTablePermissionsEqual(expectedPermissions, testPermissions);

            // Add a policy, check setting and getting.
            SharedAccessTablePolicy policy1 = new SharedAccessTablePolicy();
            Calendar now = GregorianCalendar.getInstance();
            policy1.setSharedAccessStartTime(now.getTime());
            now.add(Calendar.MINUTE, 10);
            policy1.setSharedAccessExpiryTime(now.getTime());

            policy1.setPermissions(EnumSet.of(SharedAccessTablePermissions.ADD, SharedAccessTablePermissions.QUREY,
                    SharedAccessTablePermissions.UPDATE, SharedAccessTablePermissions.DELETE));
            expectedPermissions.getSharedAccessPolicies().put(UUID.randomUUID().toString(), policy1);

            table.uploadPermissions(expectedPermissions);
            testPermissions = table.downloadPermissions();
            assertTablePermissionsEqual(expectedPermissions, testPermissions);
        }
        finally {
            // cleanup
            table.deleteIfExists();
        }
    }

    static void assertTablePermissionsEqual(TablePermissions expected, TablePermissions actual) {
        HashMap<String, SharedAccessTablePolicy> expectedPolicies = expected.getSharedAccessPolicies();
        HashMap<String, SharedAccessTablePolicy> actualPolicies = actual.getSharedAccessPolicies();
        Assert.assertEquals("SharedAccessPolicies.Count", expectedPolicies.size(), actualPolicies.size());
        for (String name : expectedPolicies.keySet()) {
            Assert.assertTrue("Key" + name + " doesn't exist", actualPolicies.containsKey(name));
            SharedAccessTablePolicy expectedPolicy = expectedPolicies.get(name);
            SharedAccessTablePolicy actualPolicy = actualPolicies.get(name);
            Assert.assertEquals("Policy: " + name + "\tPermissions\n", expectedPolicy.getPermissions().toString(),
                    actualPolicy.getPermissions().toString());
            Assert.assertEquals("Policy: " + name + "\tStartDate\n", expectedPolicy.getSharedAccessStartTime()
                    .toString(), actualPolicy.getSharedAccessStartTime().toString());
            Assert.assertEquals("Policy: " + name + "\tExpireDate\n", expectedPolicy.getSharedAccessExpiryTime()
                    .toString(), actualPolicy.getSharedAccessExpiryTime().toString());

        }

    }
}
