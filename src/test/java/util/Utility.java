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
        JSONObject jsonObject = getJsonObject();
        Job queryJob =
                Utility.bqTableQuery(HeaderAndBqConstant.queryForFetchingSingleData.replace("tableName", HeaderAndBqConstant.tableName));
        TableResult testEventResult = queryJob.getQueryResults();
        for (FieldValueList row : testEventResult.iterateAll()) {
            Assert.assertEquals(jsonObject.get(HeaderAndBqConstant.iframeId), row.get(HeaderAndBqConstant.iframeId).getValue());
            Assert.assertEquals(jsonObject.get(HeaderAndBqConstant.password), row.get(HeaderAndBqConstant.password).getValue());
            Assert.assertEquals(jsonObject.get(HeaderAndBqConstant.registrant_sex), row.get(HeaderAndBqConstant.registrant_sex.replace(".","_")).getValue());
            Assert.assertEquals(jsonObject.get(HeaderAndBqConstant.registrant_hcn), row.get(HeaderAndBqConstant.registrant_hcn.replace(".","_")).getValue());
            Assert.assertEquals(jsonObject.get("registrant.firstName"), row.get("registrant_firstName").getValue());
            Assert.assertEquals(jsonObject.get("registrant.lastName"), row.get("registrant_lastName").getValue());
            Assert.assertEquals(jsonObject.get("registrant.dob"), row.get("registrant_dob").getValue());
            Assert.assertEquals(jsonObject.get("registrant.sex"), row.get("registrant_sex").getValue());
            Assert.assertEquals(jsonObject.get("registrant.vaccinationProvince"), row.get("registrant_vaccinationProvince").getValue());
            Assert.assertEquals(jsonObject.get("registrant.dobClean"), row.get("registrant_dobClean").getValue());
            Assert.assertEquals(jsonObject.get("registrant.city"), row.get("registrant_city").getValue());
            Assert.assertEquals(jsonObject.get("registrant.doseDate"), row.get("registrant_doseDate").getValue());
            Assert.assertEquals(jsonObject.get("registrant.contactEmail"), row.get("registrant_contactEmail").getValue());
            Assert.assertEquals(jsonObject.get("registrant.emailClean"), row.get("registrant_emailClean").getValue());
            Assert.assertEquals(jsonObject.get("agent.relationship"), row.get("agent_relationship").getValue());
            Assert.assertEquals(jsonObject.get("agent.firstName"), row.get("agent_firstName").getValue());
            Assert.assertEquals(jsonObject.get("agent.lastName"), row.get("agent_lastName").getValue());
            Assert.assertEquals(jsonObject.get("agent.phone"), row.get("agent_phone").getValue());
            Assert.assertEquals(jsonObject.get("location.link"), row.get("location_link").getValue());
            Assert.assertEquals(jsonObject.get("verifyPhone"), row.get("verifyPhone").getValue());
            Assert.assertEquals(jsonObject.get("verifyEmail"), row.get("verifyEmail").getValue());
            Assert.assertEquals(((JSONObject) jsonObject.get("addressDetails")).get("Id"), row.get("addressDetails_Id").getValue());
            Assert.assertEquals(((JSONObject) jsonObject.get("addressDetails")).get("City"), row.get("addressDetails_City").getValue());
            Assert.assertEquals(((JSONObject) jsonObject.get("addressDetails")).get("Label"), row.get("addressDetails_Label").getValue());
            Assert.assertEquals(((JSONObject) jsonObject.get("addressDetails")).get("CountryName"), row.get("addressDetails_CountryName").getValue());
        }
    }

    private static JSONObject getJsonObject() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(HeaderAndBqConstant.file));
        JSONObject jsonObject = (JSONObject) obj;
        return jsonObject;
    }

}
