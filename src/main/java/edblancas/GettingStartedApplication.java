package edblancas;

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
        // TODO: application initialization
    }

    @Override
    public void run(final GettingStartedConfiguration configuration,
                    final Environment environment) {
        // TODO: implement application
    }

}
