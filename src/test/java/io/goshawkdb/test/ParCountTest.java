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
import io.goshawkdb.client.VarUUId;

public class ParCountTest extends TestBase {

    public ParCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void parCount() throws Exception {
        try {
            int threadCount = 8;
            TxnId origRootVsn = setRootToNZeroObjs(createConnections(1)[0], threadCount);

            inParallel(threadCount, (int tId, Connection c, Queue<Exception> exceptionQ) -> {
                awaitRootVersionChange(c, origRootVsn);
                VarUUId objId = c.runTransaction(txn ->
                        txn.getRoot().getReferences()[tId].id
                ).result;
                long start = System.nanoTime();
                long expected = 0L;
                for (int idx = 0; idx < 1000; idx++) {
                    long expectedCopy = expected;
                    expected = c.runTransaction(txn -> {
                        GoshawkObj obj = txn.getObject(objId);
                        ByteBuffer valBuf = obj.getValue().order(ByteOrder.BIG_ENDIAN);
                        long old = valBuf.getLong(0);
                        if (old == expectedCopy) {
                            long val = old + 1;
                            obj.set(valBuf.putLong(0, val));
                            return val;
                        } else {
                            throw new IllegalStateException("" + tId + ": Expected " + expectedCopy + " but found " + old);
                        }
                    }).result;
                }
                long end = System.nanoTime();
                System.out.println("" + tId + ": Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
            });
        } finally {
            shutdown();
        }
    }
}
