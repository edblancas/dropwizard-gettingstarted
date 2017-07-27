package edblancas;

import com.amk.dropwizard.hbase.HBaseBundle;
import com.amk.dropwizard.hbase.HBaseBundleConfiguration;
import edblancas.db.GameDao;
import edblancas.health.TemplateHealtCheck;
import edblancas.resources.GameResource;
import edblancas.resources.HelloWorldResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class GettingStartedApplication extends Application<GettingStartedConfiguration> {

    public static void main(final String[] args) throws Exception {
        new GettingStartedApplication().run(args);
    }

    @Override
    public String getName() {
        return "GettingStarted";
    }

    @Override
    public void initialize(final Bootstrap<GettingStartedConfiguration> bootstrap) {
        bootstrap.addBundle(hBaseBundle);
    }

    @Override
    public void run(final GettingStartedConfiguration configuration,
                    final Environment environment) throws Exception {
        final HelloWorldResource helloResource = new HelloWorldResource(
                configuration.getTemplate(),
                configuration.getDefaultName()
        );
        final TemplateHealtCheck healtCheck = new TemplateHealtCheck(configuration.getTemplate());

        final GameDao gameDao = new GameDao(hBaseBundle.getTable("games"));
        final GameResource gameResource = new GameResource(gameDao);

        environment.healthChecks().register("template", healtCheck);
        environment.jersey().register(helloResource);
        environment.jersey().register(gameResource);
    }

    /**
     * Anonymous class to start HbaseBundle.
     */
    private final HBaseBundle<GettingStartedConfiguration> hBaseBundle = new HBaseBundle<GettingStartedConfiguration>() {
        @Override
        protected HBaseBundleConfiguration getHBaseBundleConfiguration(GettingStartedConfiguration config) {
            return config.getHBaseBundleConfiguration();
        }
    };
}
