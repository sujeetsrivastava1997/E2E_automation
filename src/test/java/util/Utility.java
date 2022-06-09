package util;

import com.google.cloud.bigquery.*;
import constants.HeaderAndBqConstant;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Utility {

    public static Job bqTableQuery(final String query) throws InterruptedException {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        queryJob = queryJob.waitFor();
        return queryJob;
    }

    @NotNull
    public static AtomicLong beforeCount(String tableName) throws InterruptedException {
        AtomicLong countBeforeInsertion = new AtomicLong();
        TableResult beforeInsertionCountQuery =
                Utility.bqTableQuery(HeaderAndBqConstant.countQuery.replace("tableName", tableName)).getQueryResults();
        beforeInsertionCountQuery
                .getValues()
                .forEach(fieldValueList -> countBeforeInsertion.set(fieldValueList.get(0).getLongValue()));
        return countBeforeInsertion;
    }

    @NotNull
    public static AtomicLong afterCount(AtomicLong countBeforeInsertion, String tableName) throws InterruptedException {
        // Sleep so app can process message
        // Would like to find a way to poll the table until it is ready if possible
        AtomicLong countAfterInsertion = new AtomicLong();
        int timeout = 0;
        while (timeout < 10) {
            Thread.sleep(1000); // NOSONAR
            TableResult afterInsertionCountQuery =
                    Utility.bqTableQuery(HeaderAndBqConstant.countQuery.replace("tableName", tableName)).getQueryResults();
            afterInsertionCountQuery
                    .getValues()
                    .forEach(fieldValueList -> countAfterInsertion.set(fieldValueList.get(0).getLongValue()));
            if (countAfterInsertion.get() == countBeforeInsertion.get() + 1) {
                break;
            }

            timeout++;
        }

        return countAfterInsertion;
    }

    @NotNull
    public static ZonedDateTime beforeModifiedTime(String tableName) throws InterruptedException {
        AtomicReference<String> timeBeforeInsertion = new AtomicReference<>();
        TableResult beforeInsertionTimeQuery =
                Utility.bqTableQuery(
                                HeaderAndBqConstant.modificationTimeQuery.replace("tableName", tableName))
                        .getQueryResults();
        beforeInsertionTimeQuery
                .getValues()
                .forEach(fieldValueList -> timeBeforeInsertion.set(fieldValueList.get(0).getStringValue()));
        ZonedDateTime zonedBeforeDateTime = getDateTime(timeBeforeInsertion);
        return zonedBeforeDateTime;
    }

    public static ZonedDateTime afterModifiedTime(ZonedDateTime zonedBeforeDateTime, String tableName)
            throws InterruptedException {
        AtomicReference<String> timeAfterInsertion = new AtomicReference<>();
        ZonedDateTime zonedAfterDateTime = null;
        int timeout = 0;
        while (timeout < 10) {
            Thread.sleep(1000); // NOSONAR
            TableResult afterInsertionTimeQuery =
                    Utility.bqTableQuery(
                                    HeaderAndBqConstant.modificationTimeQuery.replace("tableName", tableName))
                            .getQueryResults();
            afterInsertionTimeQuery
                    .getValues()
                    .forEach(
                            fieldValueList -> timeAfterInsertion.set(fieldValueList.get(0).getStringValue()));
            zonedAfterDateTime = getDateTime(timeAfterInsertion);
            if (zonedAfterDateTime != zonedBeforeDateTime) {
                break;
            }

            timeout++;
        }
        return zonedAfterDateTime;
    }

    @NotNull
    private static ZonedDateTime getDateTime(AtomicReference<String> modificationTime) {
        ZonedDateTime zonedAfterDateTime;
        String time = String.valueOf(modificationTime.get()).split("\\.")[0];
        Instant instant = Instant.ofEpochSecond(Long.parseUnsignedLong(time));
        zonedAfterDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
        return zonedAfterDateTime;
    }

    public static void bqVerification() throws IOException, ParseException, InterruptedException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(HeaderAndBqConstant.file));
        JSONObject jsonObject = (JSONObject) obj;
        Job queryJob =
                Utility.bqTableQuery(HeaderAndBqConstant.queryForFetchingSingleData.replace("tableName", HeaderAndBqConstant.tableName));
        TableResult testEventResult = queryJob.getQueryResults();
        for (FieldValueList row : testEventResult.iterateAll()) {
            Assert.assertEquals(jsonObject.get("iframeId"), row.get("iframeId").getValue());
            Assert.assertEquals(jsonObject.get("password"), row.get("password").getValue());
            Assert.assertEquals(jsonObject.get("registrant.sex"), row.get("registrant_sex").getValue());
            Assert.assertEquals(jsonObject.get("registrant.hcn"), row.get("registrant_hcn").getValue());
        }
    }

}
