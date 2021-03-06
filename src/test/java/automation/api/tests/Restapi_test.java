package automation.api.tests;

import constants.HeaderAndBqConstant;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import util.Utility;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

/**
 * fetch the value from Bigquery before and after producing the event into pub/sub using Bigquery utility.
 * send the post request to base url and event publish on pub/sub using gatling.
 * set the GOOGLE_APPLICATION_CREDENTIALS property.
 */
public class Restapi_test extends Simulation {

    AtomicLong beforeInsertionCount;
    ZonedDateTime beforeModificationTime;
    public void before() {

        try {
            beforeInsertionCount = Utility.beforeCount(HeaderAndBqConstant.tableName);
            beforeModificationTime = (Utility.beforeModifiedTime(HeaderAndBqConstant.modificationTableName));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    HttpProtocolBuilder httpProtocolBuilder = http.baseUrl("");
    ScenarioBuilder scn1 = CoreDsl.scenario(HeaderAndBqConstant.publishMessage)
            .exec(http(HeaderAndBqConstant.message)
                    .post(HeaderAndBqConstant.baseUrl)
                    .header("Content-Type", HeaderAndBqConstant.contentType)
                    .header("x-apikey", HeaderAndBqConstant.api_key)
                    .header("organization", HeaderAndBqConstant.organization)
                    .header("team", HeaderAndBqConstant.Team)
                    .header("version", HeaderAndBqConstant.Version)
                    .header("formName", HeaderAndBqConstant.FormName)
                    .body(CoreDsl.RawFileBody(HeaderAndBqConstant.file))
                    .check(status().is(HeaderAndBqConstant.statusCode)));

    {
        setUp(scn1.injectOpen(atOnceUsers(1)).protocols(httpProtocolBuilder));
    }

    public void after() {
        try {
            AtomicLong afterInsertionCount = Utility.afterCount(beforeInsertionCount, HeaderAndBqConstant.tableName);
            ZonedDateTime afterModificationTime = Utility.afterModifiedTime(beforeModificationTime, HeaderAndBqConstant.modificationTableName);
            Assert.assertEquals(beforeInsertionCount.get() + 1, afterInsertionCount.get());
            Assert.assertNotEquals(beforeModificationTime, afterModificationTime);
            Utility.bqVerification();
        } catch (InterruptedException | IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void init() {

        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "gcloud_credentials.json");
    }

}