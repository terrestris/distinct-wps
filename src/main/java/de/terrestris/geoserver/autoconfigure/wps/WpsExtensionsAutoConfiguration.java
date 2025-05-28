package de.terrestris.geoserver.autoconfigure.wps;

import de.terrestris.geoserver.wps.DistinctValues;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.WPSFactoryExtension;
import org.geoserver.wps.gs.GeoServerProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;

/**
 * Spring-boot autoconfiguration, enabling the following WPS {@link GeoServerProcess processes}
 *
 * Vanilla GeoServer {@literal applicationContext.xml} at the time of writing:
 * <pre>
 * {@code
 * <beans>
 *   <bean id="layerDataSourceValidator" class="de.terrestris.geoserver.wps.DistinctValues">
 *     <constructor-arg index="0" ref="geoServer"/>
 *   </bean>
 * </beans>
 * }
 * </pre>
 */

@AutoConfiguration
@ConditionalOnClass(WPSFactoryExtension.class)
public class WpsExtensionsAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(WpsExtensionsAutoConfiguration.class);

    @Bean(name = "layerDataSourceValidator")
    public DistinctValues layerDataSourceValidator(GeoServer geoServer) {
        log.info("Setting GeoServer for the distinct values WPS: " + geoServer.toString());
        return new DistinctValues(geoServer);
    }

    @PostConstruct
    void log(){
        log.info("Distinct values WPS processes loaded");
    }

}
