package org.infinispan.persistence.ceph.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.ceph.CephStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.TestingUtil.InfinispanStartTag;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.ceph.configuration.XmlFileParsingTest")
public class XmlFileParsingTest<K, V> extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testXmlConfig() throws Exception {
      String config = InfinispanStartTag.LATEST +
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <persistence passivation=\"false\"> \n" +
            "         <ceph-store xmlns=\"urn:infinispan:config:store:ceph:9.0\" " +
            "          user-name=\"admin\" " +
            "          key=\"AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==\" " +
            "          monitor-host=\"192.168.122.145:6789\" " +
            "          pool-name=\"ispn-store\"" +
            "         />\n" +
            "      </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>" +
            INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(1, "v1");
            assertEquals(cache.get(1), "v1");
            @SuppressWarnings("unchecked")
            CephStore<K, V> store = (CephStore<K, V>) TestingUtil.getFirstLoader(cache);
            assertEquals(store.getConfiguration().userName(), "admin");
            assertEquals(store.getConfiguration().key(), "AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==");
            assertEquals(store.getConfiguration().monitorHost(), "192.168.122.145:6789");
            assertEquals(store.getConfiguration().poolName(), "ispn-store");
         }
      });
   }
   
   
   public void testDefaultPoolName() throws Exception {
      String config = InfinispanStartTag.LATEST +
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <persistence passivation=\"false\"> \n" +
            "         <ceph-store xmlns=\"urn:infinispan:config:store:ceph:9.0\" " +
            "          user-name=\"admin\" " +
            "          key=\"AQCY2sdXyDIcJxAAK1edRJ8xOJ2NkkiXzAuq5A==\" " +
            "          monitor-host=\"192.168.122.145:6789\" " +
            "          pool-name-prefix=\"test\" " +
            "         />\n" +
            "      </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>" +
            INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            @SuppressWarnings("unchecked")
            CephStore<K, V> store = (CephStore<K, V>) TestingUtil.getFirstLoader(cache);
            assertEquals(store.getPoolName(), "test____defaultcache");
         }
      });
   }
}