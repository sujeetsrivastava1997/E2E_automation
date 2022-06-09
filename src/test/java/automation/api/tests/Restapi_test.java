package automation.api.tests;

import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import utils.HeaderConstants;

import static io.gatling.javaapi.core.CoreDsl.rampUsers;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

public class Restapi_test extends Simulation {

    HttpProtocolBuilder httpProtocolBuilder = http.baseUrl("");

    //Hitting post call to publish message on pub/sub
    ScenarioBuilder scn1 = CoreDsl.scenario("post call to publish message on pub/sub")
            .exec(http("publish message")
                    .post("https://sdm-formservice-dev.api.banting.lblw.cloud/api/v1/form")
                    .header("Content-Type", "application/json")
                    .header("x-apikey", "Jiy473bm8LCfe09wmtBNRlq85y8c7Nf1")
                    .header("organization", HeaderConstants.Org)
                    .header("team", HeaderConstants.Team)
                    .header("version", HeaderConstants.Version)
                    .header("formName", HeaderConstants.FormName)
                    .body(CoreDsl.RawFileBody("externalfile/formbody.json"))
                    .check(status().is(200)));

    //virtual user setup for load
    {
        setUp(scn1.injectOpen(rampUsers(1).during(1))).protocols(httpProtocolBuilder);
    }
}