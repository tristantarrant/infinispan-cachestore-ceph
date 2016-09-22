package org.infinispan.persistence.ceph.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@Namespaces({
      @Namespace(uri = "urn:infinispan:config:store:ceph:9.0", root = CephStoreConfigurationParser.ROOT_ELEMENT),
      @Namespace(root = CephStoreConfigurationParser.ROOT_ELEMENT) 
})
public class CephStoreConfigurationParser implements ConfigurationParser {

   public static final String ROOT_ELEMENT = "ceph-store";
   public static final String OVERRIDES_SEPARATOR = ",";
   public static final String PROPERTY_SEPARATOR = "=";

   public CephStoreConfigurationParser() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case CEPH_STORE: {
         parseCephStore(reader, builder.persistence(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseCephStore(final XMLExtendedStreamReader reader, PersistenceConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      CephStoreConfigurationBuilder builder = new CephStoreConfigurationBuilder(loadersBuilder);
      parseCephStoreAttributes(reader, builder, classLoader);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         default: {
            Parser.parseStoreElement(reader, builder);
            break;
         }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseCephStoreAttributes(XMLExtendedStreamReader reader, CephStoreConfigurationBuilder builder,
         ClassLoader classLoader) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         String attributeName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attributeName);
         switch (attribute) {
         case USER_NAME: {
            builder.userName(value);
            break;
         }
         case KEY: {
            builder.key(value);
            break;
         }
         case KEYRING_PATH: {
            builder.keyringPath(value);
            break;
         }
         case MONITOR_HOST: {
            builder.monitorHost(value);
            break;
         }
         case POOL_NAME: {
            builder.poolName(value);
            break;
         }
         case POOL_NAME_PREFIX: {
            builder.poolNamePrefix(value);
            break;
         }
         case KEY_2_STRING_MAPPER: {
            builder.key2StringMapper(value);
            break;
         }
         default: {
            Parser.parseStoreAttribute(reader, i, builder);
            break;
         }
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
