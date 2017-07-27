package edblancas;

import com.amk.dropwizard.hbase.HBaseBundleConfiguration;
import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.*;

public class GettingStartedConfiguration extends Configuration {
    @NotEmpty
    private String template;

    @NotEmpty
    private String defaultName = "Stranger";

    private HBaseBundleConfiguration hbaseBundleConfiguration;

    @JsonProperty
    public String getTemplate() {
        return template;
    }

    @JsonProperty
    public void setTemplate(String template) {
        this.template = template;
    }

    @JsonProperty
    public String getDefaultName() {
        return defaultName;
    }

    @JsonProperty
    public void setDefaultName(String name) {
        this.defaultName = name;
    }

    @JsonProperty("hbase")
    public HBaseBundleConfiguration getHBaseBundleConfiguration() {
        return hbaseBundleConfiguration;
    }

    @JsonProperty
    public void setHBaseBundleConfiguration(final HBaseBundleConfiguration hbaseBundleConfiguration) {
        this.hbaseBundleConfiguration = hbaseBundleConfiguration;
    }
}
