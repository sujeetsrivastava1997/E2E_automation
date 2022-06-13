package constants;

public class HeaderAndBqConstant {

    public static String organization = "omni";
    public static int statusCode = 200;
    public static String publishMessage = "post call to publish message on pub/sub";
    public static String message ="publish message";
    public static String contentType = "application/json";
    public static String api_key = "Jiy473bm8LCfe09wmtBNRlq85y8c7Nf1";
    public static String baseUrl = "https://sdm-formservice-dev.api.banting.lblw.cloud/api/v1/form";
    public static String file = "src/test/resources/externalfile/formbody.json";
    public static String Team = "loblaw_QA";
    public static String Version = "1.0";
    public static String FormName = "gatling automation";
    public static String countQuery = "SELECT Count(1) FROM tableName";
    public static String queryForFetchingSingleData = "SELECT * FROM tableName LIMIT 1";
    public static String modificationTimeQuery =
            "SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time, FROM "
                    + "ld-sdm-daylight-dev"
                    + "."
                    + "sdm_form_data"
                    + ".__TABLES__ where table_id = "
                    + "'"
                    + "tableName"
                    + "'";
    public static String tableName = "`ld-sdm-daylight-dev.sdm_form_data.gatling automation`";
    public static String modificationTableName = "gatling automation";

    public static String iframeId = "iframeId";
    public static String password = "password";
    public static String registrant_sex = "registrant.sex";
    public static String registrant_hcn = "registrant.hcn";

}
