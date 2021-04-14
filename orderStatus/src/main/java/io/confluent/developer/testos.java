package io.confluent.developer;
import io.confluent.ksql.function.udf.UdfParameter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static java.time.temporal.ChronoUnit.DAYS;


public class testos {

    public testos (){}


    public static void main(String[] args) {

        updateDelDate("534,123,534,666,728","538,539,540,550,400","13142,22432,10131,987,8923");
        //updateOrderStatus(null,null,null);
        //String myString="112314";
        //convertDate(myString);
    }
    public static String updateOrderStatus(
            final Map<String, String> inputLines,
            final Integer hold_length,
          final Integer compareDate
    ) {
        int lineCount=0;
        if (isNotNullOrEmptyMap(inputLines))
        lineCount = inputLines.entrySet().size();
        String returnStatus="";
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
        System.out.println("this is returned:" + returnStatus + "trailing");
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
    public static String updateDelDate(
            final String sdlttr,
            final String sdnxtr,
            final String sdrsdj
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
        else {
            //System.out.println("0");
            return "0";
        }
    }
    public static Long convertDate(final String julian_date){
        Long millis;
        if(!isNullOrEmpty(julian_date)) {
            DateTimeFormatter dayOfYearFormatter
                    = DateTimeFormatter.ofPattern("uuuuDDD");
            LocalDate convertedDate = LocalDate.parse(Integer.toString(Integer.parseInt(julian_date) + 1900000), dayOfYearFormatter);
            millis = convertedDate.toEpochDay() * 24 * 3600 * 1000;
            return millis;
        }
        else {
            millis=null;
            return millis;

        }
    }
    public static boolean isNullOrEmptyMap(Map <String, String> map) {
        return (map == null || map.isEmpty());
    }
    public static boolean isNotNullOrEmptyMap(Map <String, String> map) {
        return !isNullOrEmptyMap(map);
    }
    public static boolean isNullOrEmpty(String str) {
        if(str != null && !str.isEmpty())
            return false;
        return true;
    }
}
