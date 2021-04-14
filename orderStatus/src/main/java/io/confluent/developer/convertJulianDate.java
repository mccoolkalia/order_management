package io.confluent.developer;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;

@UdfDescription(name = "convertJulianDate", description = "Convert a Julian Date to a regular date")
public class convertJulianDate {
    @Udf(description = "Convert a Julian Date")

    public Long convertDate(@UdfParameter(value = "julianDate", description = "Input Julian Date") final String julian_date){
        Long millis;
        if(!isNullOrEmpty(julian_date) && Integer.parseInt(julian_date)>0) {
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
    public static boolean isNullOrEmpty(String str) {
        if(str != null && !str.isEmpty())
            return false;
        return true;
    }
}



