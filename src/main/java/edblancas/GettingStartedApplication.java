package edblancas;

import com.amk.dropwizard.hbase.HBaseBundle;
import com.amk.dropwizard.hbase.HBaseBundleConfiguration;
import edblancas.health.TemplateHealtCheck;
import edblancas.resources.HelloWorldResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
                    final Environment environment) {
        final HelloWorldResource resource = new HelloWorldResource(
                configuration.getTemplate(),
                configuration.getDefaultName()
        );
        final TemplateHealtCheck healtCheck = new TemplateHealtCheck(configuration.getTemplate());
        environment.healthChecks().register("template", healtCheck);
        environment.jersey().register(resource);

        try {
            Table table = hBaseBundle.getTable("test");
            System.out.println(">>>" + table.getName());
            // Instantiating the Scan class
            Scan scan = new Scan();

            // Scanning the required columnstest
//            scan.addColumn(Bytes.toBytes(""), Bytes.toBytes(""));

            // Getting the scan result
            ResultScanner scanner = table.getScanner(scan);

            // Reading values from scan result
            for (Result result = scanner.next(); result != null; result = scanner.next())

                System.out.println("Found row : " + result);
            //closing the scanner
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Anonymous class to start HbaseBundle.
     */
    private final HBaseBundle<GettingStartedConfiguration> hBaseBundle = new HBaseBundle<GettingStartedConfiguration>() {

        /* (non-Javadoc)
         * @see com.amk.dropwizard.hbase.HBaseBundle#getHBaseBundleConfigurationn(io.dropwizard.Configuration)
         */
        @Override
        protected HBaseBundleConfiguration getHBaseBundleConfigurationn(GettingStartedConfiguration config) {
            return config.getHBaseBundleConfiguration();
        }

    };
}
