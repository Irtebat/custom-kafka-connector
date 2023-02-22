package ir.connector;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
public class CustomConnectorTask extends SourceTask {
    private final Random random = new Random(System.currentTimeMillis());
    private final Logger log = LoggerFactory.getLogger(CustomConnectorTask.class);

    private int taskSleepTimeout;
    private List<String> sources;
    private Schema recordSchema;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> properties) {
        //taskSleepTimeout = Integer.parseInt(properties.get("TASK_SLEEP_TIMEOUT_CONFIG"));
        taskSleepTimeout = 1000;
        String sourcesStr = properties.get("SOURCES");
        sources = Arrays.asList(sourcesStr.split(","));
        recordSchema = SchemaBuilder.struct()
                .field("Column-A", Schema.STRING_SCHEMA).required()
                .field("Column-B", Schema.INT32_SCHEMA).required()
                .build();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(taskSleepTimeout);
        List<SourceRecord> records = new ArrayList<>();
        for (String source : sources) {
            log.info("Polling data from the source '" + source + "'");
            records.add(new SourceRecord(
                    Collections.singletonMap("source", source),
                    Collections.singletonMap("offset", 0),
                    source, null, null, null,
                    recordSchema, createStruct(recordSchema)));
        }
        return records;
    }

    private Struct createStruct(Schema schema) {
        Struct struct = new Struct(schema);
        struct.put("Column-A", randomString());
        struct.put("Column-B", random.nextInt(100));
        return struct;
    }

    private String randomString() {
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));
        return generatedString;
    }

    @Override
    public void stop() {
    }

}
