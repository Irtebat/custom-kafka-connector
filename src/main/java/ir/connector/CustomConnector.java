package ir.connector;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

public class CustomConnector extends SourceConnector {

    private final Logger log = LoggerFactory.getLogger(CustomConnector.class);

    private Map<String, String> props;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(
                        "FIRST_REQUIRED_PARAMETER",
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "first required parameter : value 1 for message ")
                .define(
                        "SECOND_REQUIRED_PARAMETER",
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "second required parameter : value 2 for message ")
                .define(
                        "FIRST_OPTIONAL_PARAMETER",
                        ConfigDef.Type.STRING,
                        "default value 1",
                        ConfigDef.Importance.HIGH,
                        "first optional parameter : optional value 1 for message")
                .define(
                        "TASK_SLEEP_TIMEOUT_CONFIG",
                        ConfigDef.Type.INT,
                        100,
                        ConfigDef.Importance.HIGH,
                        "Sleep timeout used by tasks during each poll");
        return configDef;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        // The partitions below represent the source's part that
        // would likely to be broken down into tasks... such as
        // tables in a database.
        
        List<String> partitions = Arrays.asList("source-1", "source-2", "source-3");

        if (partitions.isEmpty()) {
            taskConfigs = Collections.emptyList();
            log.warn("No tasks created because there is zero to work on");
        } else {
            int numTasks = Math.min(partitions.size(), maxTasks);
            List<List<String>> partitionSources = ConnectorUtils.groupPartitions(partitions, numTasks);
            for (List<String> source : partitionSources) {
                Map<String, String> taskConfig = new HashMap<>(props);
                taskConfig.put("SOURCES", String.join(",", source));
                taskConfigs.add(taskConfig);
            }
        }
        return taskConfigs;
    }

    @Override
    public void stop() {

    }

}
