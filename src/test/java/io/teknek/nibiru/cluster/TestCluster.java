package io.teknek.nibiru.cluster;

import java.util.*;
import java.util.concurrent.*;


import io.teknek.nibiru.ServerShutdown;
import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.*;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.tunit.TUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Sets;

public class TestCluster extends ServerShutdown {

    @Rule
    public TemporaryFolder node1Folder = new TemporaryFolder();

    @Rule
    public TemporaryFolder node2Folder = new TemporaryFolder();

    @Rule
    public TemporaryFolder node3Folder = new TemporaryFolder();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void letTwoNodesDiscoverEachOther() throws InterruptedException, ClientException {
        final Server[] s = new Server[3];
        {
            Configuration conf = TestUtil.aBasicConfiguration(node1Folder);
            Map<String, Object> clusterProperties = new HashMap<>();
            conf.setClusterMembershipProperties(clusterProperties);
            conf.setTransportHost("127.0.0.1");
            clusterProperties.put(GossipClusterMembership.HOSTS, List.of("127.0.0.1"));
            s[0] = registerServer(new Server(conf));
        }
        {
            Configuration conf = TestUtil.aBasicConfiguration(node2Folder);
            Map<String, Object> clusterProperties = new HashMap<>();
            conf.setClusterMembershipProperties(clusterProperties);
            clusterProperties.put(GossipClusterMembership.HOSTS, List.of("127.0.0.1"));
            conf.setTransportHost("127.0.0.2");
            s[1] = registerServer(new Server(conf));
        }
        {
            Configuration conf = TestUtil.aBasicConfiguration(node3Folder);
            Map<String, Object> clusterProperties = new HashMap<>();
            conf.setClusterMembershipProperties(clusterProperties);
            clusterProperties.put(GossipClusterMembership.HOSTS, List.of("127.0.0.1"));
            conf.setTransportHost("127.0.0.3");
            s[2] = registerServer(new Server(conf));
        }
        for (Server server : s) {
            server.init();
        }
        TUnit.assertThat((Callable) () -> s[2].getClusterMembership().getLiveMembers().size())
                .afterWaitingAtMost(11, TimeUnit.SECONDS).isEqualTo(2);
        Assert.assertEquals(2, s[2].getClusterMembership().getLiveMembers().size());
        Assert.assertEquals("127.0.0.1", s[2].getClusterMembership().getLiveMembers().get(0).getHost());
        MetaDataClient c = new MetaDataClient("127.0.0.1", s[1].getConfiguration().getTransportPort(), 20000, 20000);
        c.createOrUpdateKeyspace("abc", new HashMap<>(), true);
        for (final Server server : s) {
            TUnit.assertThat((Callable) () -> server.getKeyspaces().get("abc") != null)
                    .afterWaitingAtMost(1000, TimeUnit.MILLISECONDS).isEqualTo(true);
        }
        Map<String, Object> stuff = new HashMap<String, Object>();
        stuff.put(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
        c.createOrUpdateStore("abc", "def", stuff, true);
        Thread.sleep(1000);
        for (Server server : s) {
            Assert.assertNotNull(server.getKeyspaces().get("abc").getStores().get("def"));
            Set<String> livingHosts = new TreeSet<>();
            for (ClusterMember cm : c.getLiveMembers()) {
                livingHosts.add(cm.getHost());
            }
            Assert.assertEquals(Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3"), livingHosts);
        }


        class X implements Callable<Void>{
            private Server[] servers;
            Session session;

            X(Server[] x) {
                this.servers = x;
                ColumnFamilyClient client = new ColumnFamilyClient(new Client("127.0.0.1",
                        Arrays.stream(s).findAny().get().getConfiguration().getTransportPort(), 10000, 10000));
                session = client.createBuilder().withKeyspace("abc")
                        .withStore("def")
                        .build();
            }

            public Void call() {
                for (long i = 0; i < 3000; i++) {
                    final Long j = i;
                    try {
                        session.put("jack" + j, "age", "5", j);
                    } catch (ClientException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }

        }

        long start = System.currentTimeMillis();
        X myX = new X(s);
        X myY = new X(s);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()){
            executor.submit(myX);
            executor.submit(myY);
        }
        long total = System.currentTimeMillis() - start;
        System.out.println("total "+ total);


    }
}
