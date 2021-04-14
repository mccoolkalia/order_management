package io.confluent.developer;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Map;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import static java.time.temporal.ChronoUnit.DAYS;

@UdfDescription(name = "updateOrderStatus", description = "Update an Order Status by parsing through Order Line Status")
public class orderStatus {
    @Udf(description = "Update the order status")
    public String updateOrderStatus(
            @UdfParameter(value = "lines", description = "input map") final Map<String, String> inputLines,
            @UdfParameter(value = "holdlength", description = "hold length") final Integer hold_length,
            @UdfParameter(value = "comparedate", description = "Compare Date") final Integer compareDate
            ) {
            int lineCount=0;
            if (isNotNullOrEmptyMap(inputLines))
                lineCount = inputLines.entrySet().size();
            String returnStatus="In Progress";
            if (lineCount>0) {

                String[] sdInty = new String[lineCount];
                String[] sdnxtr = new String[lineCount];
                String[] sdlttr = new String[lineCount];
                String[] sdsobk = new String[lineCount];

                int i = 0;
                int sdIntyEqualsS = 0;
                int sdnxtrEquals999 = 0;
                int sdlttrEquals620 = 0;
                int sdlttrEquals980 = 0;
                int sdsobkAndsdnxtr = 0;
                //return inputLines.get("1099") + " followed by " + inputLines.get("1333") + " and " + inputLines.get("1045");
                //{1099=500|45|4, 1333=500|45|4, 1045=500|45|4}
                //return inputLines.substring(1,4);

                for (Map.Entry<String, String> entry : inputLines.entrySet()) {
                    int sep1 = entry.getValue().indexOf("|");
                    int sep2 = entry.getValue().indexOf("|", sep1 + 1);
                    int sep3 = entry.getValue().indexOf("|", sep2 + 1);
                    if(sep1>0)
                        sdInty[i] = entry.getValue().substring(0, sep1);
                    else
                        sdInty[i] = "";
                    if(sep1>0 && sep2>0)
                        sdnxtr[i] = entry.getValue().substring(sep1 + 1, sep2);
                    else
                        sdnxtr[i]="";
                    if(sep2>0 && sep3>0)
                        sdlttr[i] = entry.getValue().substring(sep2 + 1, sep3);
                    else sdlttr[i] = "";
                    if (sep3>0)
                        sdsobk[i] = entry.getValue().substring(sep3 + 1, entry.getValue().length());
                    else
                        sdsobk[i] = "";
                    if (sdInty[i].trim().equals("S")) sdIntyEqualsS++;
                    if (sdnxtr[i].trim().equals("999")) sdnxtrEquals999++;
                    if (sdlttr[i].trim().equals("620")) sdlttrEquals620++;
                    if (sdlttr[i].trim().equals("980")) sdlttrEquals980++;
                    if (Integer.parseInt(sdsobk[i].trim()) >= 1 && Integer.parseInt(sdnxtr[i].trim()) <= 527)
                        sdsobkAndsdnxtr++;
                    i++;
                }
                if (sdIntyEqualsS > 0) {
                    if (sdnxtrEquals999 == lineCount && sdlttrEquals620 == lineCount) {
                        returnStatus = "Delivered";
                    } else if (sdnxtrEquals999 == lineCount && sdlttrEquals980 == lineCount) {
                        returnStatus = "Cancelled";
                    } else if (sdsobkAndsdnxtr > 0 && compareDate>0 && calcHoldLag(compareDate) < 3) {
                        returnStatus = "Product Unavailable";
                    } else if (hold_length > 0) {
                        returnStatus = "On Hold";
                    } else returnStatus = "In Progress";

                } else if (sdIntyEqualsS == 0) {
                    if (sdnxtrEquals999 == lineCount && sdlttrEquals620 == lineCount) {
                        returnStatus = "Processed";
                    } else if (sdnxtrEquals999 == lineCount && sdlttrEquals980 == lineCount) {
                        returnStatus = "Cancelled";
                    } else if (hold_length > 0) {
                        returnStatus = "On Hold";
                    } else returnStatus = "In Progress";
                }
            }
        return returnStatus;
    }
    public static Long calcHoldLag(Integer inputDate){
        LocalDate today = LocalDate.now();
        DateTimeFormatter dayOfYearFormatter
                = DateTimeFormatter.ofPattern("uuuuDDD");
        DateTimeFormatter yyyymmddFormatter
                = DateTimeFormatter.ofPattern("uuuu-MM-dd");


        LocalDate incoming = LocalDate.parse(Integer.toString(inputDate+1900000), dayOfYearFormatter);
        Long holdLag =DAYS.between(incoming,today);
        return holdLag;

    }
    public static boolean isNullOrEmptyMap(Map <String, String> map) {
        return (map == null || map.isEmpty());
    }
    public static boolean isNotNullOrEmptyMap(Map <String, String> map) {
        return !isNullOrEmptyMap(map);
    }
}


