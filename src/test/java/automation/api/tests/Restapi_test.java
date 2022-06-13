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
        return ;
    }

    HttpProtocolBuilder httpProtocolBuilder = http.baseUrl("");
    ScenarioBuilder scn1 = CoreDsl.scenario("post call to publish message on pub/sub")
            .exec(http("publish message")
                    .post("https://sdm-formservice-dev.api.banting.lblw.cloud/api/v1/form")
                    .header("Content-Type", "application/json")
                    .header("x-apikey", "Jiy473bm8LCfe09wmtBNRlq85y8c7Nf1")
                    .header("organization", HeaderAndBqConstant.Org)
                    .header("team", HeaderAndBqConstant.Team)
                    .header("version", HeaderAndBqConstant.Version)
                    .header("formName", HeaderAndBqConstant.FormName)
                    .body(CoreDsl.RawFileBody(HeaderAndBqConstant.file))
                    .check(status().is(200)));

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