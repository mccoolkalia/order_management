package io.confluent.developer;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.ArrayList;
import java.util.Collections;

@UdfDescription(name = "updateDeliveryDate", description = "Update the confirmed Delivery Date")
public class updateDeliveryDate {
    @Udf(description = "Update a Delivery Date")
    public String updateDelDate(
            @UdfParameter(value = "sdlttr", description = "all sdlttr values") final String sdlttr,
            @UdfParameter(value = "sdnxtr", description = "all sdnxtr values") final String sdnxtr,
            @UdfParameter(value = "sdrsdj", description = "all sdrsdj values") final String sdrsdj
            ){
        int lineCount=0;
        if(!isNullOrEmpty(sdlttr) && !isNullOrEmpty(sdnxtr) && !isNullOrEmpty(sdlttr)) {
            String sdlttr_array[] = sdlttr.split(",");
            String sdnxtr_array[] = sdnxtr.split(",");
            String sdrsdj_array[] = sdrsdj.split(",");
            lineCount=sdlttr_array.length;
            ArrayList<Integer> qualifyingDates = new ArrayList<Integer>();
            for (int i = 0; i < lineCount; i++) {
                if (sdlttr_array[i].equals("534")  && Integer.parseInt(sdnxtr_array[i]) >= 538) {
                    qualifyingDates.add(Integer.parseInt(sdrsdj_array[i]));
                }
            }
            Collections.sort(qualifyingDates);
            //System.out.println(qualifyingDates.get(0));
            return Integer.toString(qualifyingDates.get(0));
        }
        else return "0";
    }
    public static boolean isNullOrEmpty(String str) {
        if(str != null && !str.isEmpty())
            return false;
        return true;
    }
}
