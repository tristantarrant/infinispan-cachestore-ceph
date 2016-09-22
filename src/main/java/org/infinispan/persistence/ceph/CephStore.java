package org.infinispan.persistence.ceph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.util.Util;
import org.infinispan.executors.ExecutorAllCompletionService;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.ceph.configuration.CephStoreConfiguration;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;

import com.ceph.rados.IoCTX;
import com.ceph.rados.ListCtx;
import com.ceph.rados.Rados;
import com.ceph.rados.ReadOp;
import com.ceph.rados.ReadOp.ReadResult;
import com.ceph.rados.exceptions.RadosException;

/**
 * Cache store implementation which allows to store cache entries into <a href="http://ceph.com/">Ceph</a> cluster.
 * It leverages <a href="http://docs.ceph.com/docs/hammer/rados/api/librados-intro/">librados</a> for communication with Ceph cluster.  
 * 
 * @author vjuranek
 * @since 9.0
 *
 */
@ConfiguredBy(CephStoreConfiguration.class)
public class CephStore<K, V> implements AdvancedLoadWriteStore<K, V> {
   //TODO logging, once messages ids are reserved
   //private static final Log log = LogFactory.getLog(CephStore.class, Log.class);

   private static final String CEPH_CONF_MONITOR_HOST = "mon host";
   private static final String CEPH_CONF_KEY = "key";
   private static final String CEPH_CONF_KEYRING = "keyring";

   private static final int CEPH_ENOENT = -2; //No such file or directory
   private static final int CEPH_ENODATA = -61; //No data available

   private static final String ATTR_LIFESPAN = "ispn_meta_lifespan";
   private static final String ATTR_MAX_IDLE = "ispn_meta_max_idle";
   private static final String ATTR_CREATED = "ispn_meta_created";
   private static final String ATTR_LAST_USED = "ispn_meta_last_used";

   private static final int POOL_BATCH_SIZE = 1000;
   private static final int PROCESS_BATCH_SIZE = 1000;

   private InitializationContext initializationContext;
   private CephStoreConfiguration configuration;
   private String poolName;

   private Rados cephCluster;
   private IoCTX poolCtx;
   private MarshallingTwoWayKey2StringMapper key2StringMapper;

   public CephStoreConfiguration getConfiguration() {
      return configuration;
   }

   public String getPoolName() {
      return poolName;
   }

   @Override
   public void init(InitializationContext initializationContext) {
      this.initializationContext = initializationContext;
      this.configuration = initializationContext.getConfiguration();
      String confPoolName = configuration.poolName();
      this.poolName = (confPoolName != null && !confPoolName.isEmpty()) ? confPoolName
            : configuration.poolNamePrefix() + "_"
                  + initializationContext.getCache().getName().replaceAll("[^a-zA-Z0-9-_\\.]", "_");
   }

   @Override
   public void start() {
      cephCluster = new Rados(configuration.userName());
      //connect to the Ceph cluster
      try {
         cephCluster.confSet(CEPH_CONF_MONITOR_HOST, configuration.monitorHost());
         String authKey = configuration.key();
         if (authKey != null && !authKey.isEmpty()) {
            cephCluster.confSet(CEPH_CONF_KEY, authKey);
         } else {
            cephCluster.confSet(CEPH_CONF_KEYRING, configuration.keyringPath());
         }
         cephCluster.connect();
      } catch (RadosException e) {
         throw new PersistenceException("Unable to connect to Ceph cluster", e);
      }

      //get of create Ceph pool
      try {
         cephCluster.poolLookup(getPoolName());
      } catch (RadosException e) {
         if (e.getReturnValue() == CEPH_ENOENT) {
            //try create pool
            try {
               cephCluster.poolCreate(getPoolName());
            } catch (RadosException ex) {
               throw new PersistenceException(String.format("Unable to create pool %s", getPoolName()), e);
            }
         } else {
            throw new PersistenceException(String.format("Cannot connect to pool %s", getPoolName()), e);
         }
      }

      //obtain pool context
      try {
         poolCtx = cephCluster.ioCtxCreate(getPoolName());
      } catch (RadosException e) {
         throw new PersistenceException("Unable to get Ceph context", e);
      }

      key2StringMapper = Util.getInstance(configuration.key2StringMapper(),
            initializationContext.getCache().getAdvancedCache().getClassLoader());
      key2StringMapper.setMarshaller(initializationContext.getMarshaller());
   }

   public void stop() {
      cephCluster.shutDown();
   }

   public int size() {
      try {
         return (int) poolCtx.poolStat().num_objects;
      } catch (RadosException e) {
         throw new PersistenceException("Cannot get poll size", e);
      }
   }

   public void clear() {
      try {
         for (String item : poolCtx.listObjects()) {
            poolCtx.remove(item);
         }
      } catch (RadosException e) {
         throw new PersistenceException(String.format("Unable to clear the pool '%s'", getPoolName()), e);
      }
   }

   public boolean contains(Object key) {
      return load(key) != null;
   }

   public MarshalledEntry<K, V> load(Object key) {
      String keyStr = key2StringMapper.getStringMapping(key);
      try {
         poolCtx.stat(keyStr);
      } catch (RadosException e) {
         if (e.getReturnValue() == CEPH_ENOENT) {
            return null;
         }
         throw new PersistenceException(String.format("Cannot stat key %s in pool %s",
               key2StringMapper.getStringMapping(key), getPoolName(), e));
      }

      long now = initializationContext.getTimeService().wallClockTime();
      Metadata metadata = loadMetadata(keyStr);
      InternalMetadata internalMetadata = loadInternalMetadata(keyStr, metadata);
      MarshalledEntry<K, V> me = internalMetadata.isExpired(now) ? null
            : initializationContext.getMarshalledEntryFactory().newMarshalledEntry(key, loadValue(keyStr),
                  new InternalMetadataImpl(metadata, now, now));
      return me;
   }

   public void write(MarshalledEntry<? extends K, ? extends V> entry) {
      String key = key2StringMapper.getStringMapping(entry.getKey());
      InternalMetadata metadata = entry.getMetadata();
      String lifespan = metadata == null ? "-1" : String.valueOf(metadata.lifespan());
      String maxIdle = metadata == null ? "-1" : String.valueOf(metadata.maxIdle());
      String created = metadata == null ? "-1" : String.valueOf(metadata.created());
      String lastUsed = metadata == null ? "-1" : String.valueOf(metadata.lastUsed());

      try {
         poolCtx.write(key, marshall(entry));
         poolCtx.setExtentedAttribute(key, ATTR_LIFESPAN, lifespan);
         poolCtx.setExtentedAttribute(key, ATTR_MAX_IDLE, maxIdle);
         poolCtx.setExtentedAttribute(key, ATTR_CREATED, created);
         poolCtx.setExtentedAttribute(key, ATTR_LAST_USED, lastUsed);
      } catch (RadosException e) {
         throw new PersistenceException(String.format("Unable to write entry %s to the pool '%s'", key, getPoolName()),
               e);
      } catch (IOException | InterruptedException e) {
         throw new PersistenceException(e);
      }
   }

   public boolean delete(Object key) {
      String keyStr = key2StringMapper.getStringMapping(key);
      try {
         poolCtx.remove(keyStr);
         return true;
      } catch (RadosException e) {
         if (e.getReturnValue() == CEPH_ENOENT) {
            return false;
         }
         throw new PersistenceException(
               String.format("Unable to delete entry %s from the pool '%s'", keyStr, getPoolName()), e);
      }
   }

   public void purge(Executor executor, PurgeListener<? super K> listener) {
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
      try {
         ListCtx purgeCtx = poolCtx.listObjectsPartial(POOL_BATCH_SIZE);
         while (purgeCtx.nextObjects() > 0) {
            submitPurgeTask(eacs, purgeCtx.getObjects(), listener);
         }
      } catch (RadosException e) {
         throw new PersistenceException(String.format("Error when purgine pool '%s'", getPoolName()), e);
      }

   }

   public void process(KeyFilter<? super K> keyFilter, CacheLoaderTask<K, V> task, Executor executor, boolean loadValue,
         boolean loadMetadata) {
      ExecutorAllCompletionService eacs = new ExecutorAllCompletionService(executor);
      final TaskContext taskContext = new TaskContextImpl();

      try {
         ListCtx processCtx = poolCtx.listObjectsPartial(POOL_BATCH_SIZE);
         Map<String, K> keyEntries = new HashMap<String, K>(PROCESS_BATCH_SIZE);
         while (processCtx.nextObjects() > 0) {
            for (String k : processCtx.getObjects()) {
               K key = (K) key2StringMapper.getKeyMapping(k);
               if (keyFilter == null || keyFilter.accept(key)) {
                  keyEntries.put(k, key);
               }
               if (keyEntries.size() == PROCESS_BATCH_SIZE) {
                  final Map<String, K> batch = keyEntries;
                  keyEntries = new HashMap<String, K>(PROCESS_BATCH_SIZE);
                  submitProcessTask(eacs, task, taskContext, batch, loadValue, loadMetadata);
               }
            }
         }

         if (!keyEntries.isEmpty()) {
            submitProcessTask(eacs, task, taskContext, keyEntries, loadValue, loadMetadata);
         }

         eacs.waitUntilAllCompleted();
         if (eacs.isExceptionThrown()) {
            throw new PersistenceException("Processing of the entries has failed", eacs.getFirstException());
         }

      } catch (RadosException e) {
         throw new PersistenceException(String.format("Error when processing pool '%s'", getPoolName()), e);
      }
   }

   private byte[] marshall(MarshalledEntry<? extends K, ? extends V> entry) throws IOException, InterruptedException {
      return initializationContext.getMarshaller().objectToByteBuffer(entry.getValue());
   }

   private V unmarshall(byte[] bytes) throws IOException, ClassNotFoundException {
      if (bytes == null) {
         return null;
      }
      return (V) initializationContext.getMarshaller().objectFromByteBuffer(bytes);
   }

   private V loadValue(String key) throws PersistenceException {
      long valueSize = 0l;
      try {
         valueSize = poolCtx.stat(key).getSize();
      } catch (RadosException e) {
         if (e.getReturnValue() == CEPH_ENOENT) {
            return null;
         }
         throw new PersistenceException(String.format("Cannot stat key %s in pool %s",
               key2StringMapper.getStringMapping(key), getPoolName(), e));
      }

      try (final ReadOp rop = poolCtx.readOpCreate()) {
         ReadResult readRes = rop.queueRead(0, valueSize);
         rop.operate(key, Rados.OPERATION_NOFLAG);
         byte[] bytes = new byte[(int) readRes.getBytesRead()];
         readRes.getBuffer().get(bytes);
         return (V) unmarshall(bytes);
      } catch (RadosException e) {
         throw new PersistenceException(String.format("Unable to read entry %s from the pool '%s'", key, getPoolName()),
               e);
      } catch (IOException | ClassNotFoundException e) {
         throw new PersistenceException(e);
      }
   }

   private Metadata loadMetadata(String key) throws PersistenceException {
      String lifespanAttr = loadAttribute(key, ATTR_LIFESPAN);
      String maxIdleAttr = loadAttribute(key, ATTR_MAX_IDLE);
      Long lifespan = lifespanAttr != null ? Long.parseLong(lifespanAttr) : -1;
      Long maxIdle = maxIdleAttr != null ? Long.parseLong(maxIdleAttr) : -1;
      return new EmbeddedMetadata.Builder().lifespan(lifespan, TimeUnit.MILLISECONDS).maxIdle(maxIdle).build();
   }

   private InternalMetadata loadInternalMetadata(String key, Metadata metadata) {
      String createdAttr = loadAttribute(key, ATTR_CREATED);
      String lastUsedAttr = loadAttribute(key, ATTR_LAST_USED);
      Long created = createdAttr != null ? Long.parseLong(createdAttr) : -1;
      Long lastUsed = lastUsedAttr != null ? Long.parseLong(lastUsedAttr) : -1;
      return new InternalMetadataImpl(loadMetadata(key), created, lastUsed);
   }

   private String loadAttribute(String key, String attrName) throws PersistenceException {
      String attr = null;
      try {
         attr = poolCtx.getExtentedAttribute(key, attrName);
      } catch (RadosException e) {
         if (e.getReturnValue() != CEPH_ENODATA) {
            throw new PersistenceException(
                  String.format("Error when reading attributes for key %s from pool '%s'", key, getPoolName()), e);
         }
      }
      return attr;
   }

   private void submitPurgeTask(CompletionService<Void> cs, final String[] keys,
         final PurgeListener<? super K> listener) {
      cs.submit(new Callable<Void>() {
         @Override
         public Void call() throws RadosException {
            long now = initializationContext.getTimeService().wallClockTime();
            for (String key : keys) {
               InternalMetadata internalMetadata = loadInternalMetadata(key, loadMetadata(key));
               if (internalMetadata.isExpired(now)) {
                  poolCtx.remove(key);
                  listener.entryPurged((K) key2StringMapper.getKeyMapping(key));
               }
            }
            return null;
         }
      });
   }

   private void submitProcessTask(CompletionService<Void> cs, final CacheLoaderTask<K, V> task,
         final TaskContext taskContext, final Map<String, K> batch, final boolean loadEntry,
         final boolean loadMetadata) {
      cs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            long now = initializationContext.getTimeService().wallClockTime();
            for (Entry<String, K> keyEntry : batch.entrySet()) {
               if (taskContext.isStopped()) {
                  break;
               }

               V value = loadEntry ? loadValue(keyEntry.getKey()) : null;
               InternalMetadata internalMetadata = loadMetadata
                     ? new InternalMetadataImpl(loadMetadata(keyEntry.getKey()), now, now) : null;
               MarshalledEntry<K, V> me = initializationContext.getMarshalledEntryFactory()
                     .newMarshalledEntry(keyEntry.getKey(), value, internalMetadata);
               if (!internalMetadata.isExpired(now)) {
                  task.processEntry(me, taskContext);
               }
            }
            return null;
         }
      });
   }

}
