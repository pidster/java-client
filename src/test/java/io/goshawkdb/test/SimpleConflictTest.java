package io.goshawkdb.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Queue;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TxnId;

public class SimpleConflictTest extends TestBase {
    public SimpleConflictTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void simpleConflict() throws Exception {
        try {
            long limit = 1000;
            int parCount = 5;
            int objCount = 3;

            TxnId rootOrigVsn = setRootToNZeroObjs(createConnections(1)[0], objCount);

            inParallel(parCount, (int tId, Connection conn, Queue<Exception> exceptionQ) -> {
                awaitRootVersionChange(conn, rootOrigVsn);
                long expected = 0L;
                ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                while (expected <= limit) {
                    long expectedCopy = expected;
                    long read = conn.runTransaction(txn -> {
                        System.out.println("" + tId + ": starting with expected " + expectedCopy);
                        GoshawkObj[] objs = txn.getRoot().getReferences();
                        long val = objs[0].getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                        if (val > limit) {
                            return val;
                        }
                        buf.putLong(0, val + 1);
                        objs[0].set(buf);
                        for (int idx = 1; idx < objs.length; idx++) {
                            long vali = objs[idx].getValue().order(ByteOrder.BIG_ENDIAN).getLong(0);
                            if (val == vali) {
                                objs[idx].set(buf);
                            } else {
                                throw new IllegalStateException("" + tId + ": Object 0 has value " + val + " but " + idx + " has value " + vali);
                            }
                        }
                        return val + 1;
                    }).result;
                    if (read < expected) {
                        throw new IllegalStateException("" + tId + ": expected to read " + expected + " but read " + read);
                    }
                    expected = read + 1;
                }
            });
        } finally {
            shutdown();
        }
    }


}
