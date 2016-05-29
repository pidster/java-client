package io.goshawkdb.test;

/**
 * @author pidster
 */

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileReader;

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;
import io.goshawkdb.client.TransactionResult;

import static io.goshawkdb.test.TestBase.getEnv;

/**
 * @author pidster
 */
public class SimpleSetupTest {

    @Test
    public void test() throws Exception {
        String clusterCertPath = getEnv("CLUSTER_CERT");
        String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        Certs certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        String hostStr = getEnv("CLUSTER_HOSTS");
        String[] hosts = hostStr.split(",");

        ConnectionFactory factory = new ConnectionFactory();

        // try-with-resources auto-closes the connection
        try (Connection connection = factory.connect(certs, hosts[0])) {
            TransactionResult<Integer> result = connection.runTransaction(t -> 1);
        }
    }
}
