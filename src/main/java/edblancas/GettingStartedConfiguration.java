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

    private HBaseBundleConfiguration hBaseBundleConfiguration;

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

    @JsonProperty
    public HBaseBundleConfiguration getHBaseBundleConfiguration() {
        return hBaseBundleConfiguration;
    }

    @JsonProperty
    public void setHBaseBundleConfiguration(HBaseBundleConfiguration hBaseBundleConfiguration) {
        this.hBaseBundleConfiguration = hBaseBundleConfiguration;
    }
}
