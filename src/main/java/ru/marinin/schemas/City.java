package ru.marinin.schemas;

import org.apache.spark.sql.types.StructType;

/**
 * Created by prochiy on 29.07.18.
 */
public class City {

    public static final String DT = "dt";
    public static final String AVERAGE_TEMPERATURE = "AverageTemperature";
    public static final String AVERAGE_TEMPERATURE_UNCERTAINTY = "AverageTemperatureUncertainty";
    public static final String CITY = "City";
    public static final String COUNTRY = "Country";
    public static final String LATITUDE = "Latitude";
    public static final String LONGTITUDE  = "Longtitude";

    public static final StructType schema = new StructType()
            .add(DT, "String")
            .add(AVERAGE_TEMPERATURE, "double")
            .add(AVERAGE_TEMPERATURE_UNCERTAINTY, "double")
            .add(CITY, "String")
            .add(COUNTRY, "String")
            .add(LATITUDE, "String")
            .add(LONGTITUDE, "String");
}
