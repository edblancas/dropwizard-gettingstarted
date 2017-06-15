package edblancas;

import edblancas.health.TemplateHealtCheck;
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
        // TODO: application initialization
    }

    @Override
    public void run(final GettingStartedConfiguration configuration,
                    final Environment environment) {
        final HelloWorldResource resource = new HelloWorldResource(
                configuration.getTemplate(),
                configuration.getDefaultName()
        );
        final TemplateHealtCheck healtCheck = new TemplateHealtCheck(configuration.getTemplate());
        environment.healthChecks().register("template", healtCheck);
        environment.jersey().register(resource);
    }

}
