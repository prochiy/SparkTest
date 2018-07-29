package ru.marinin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.marinin.schemas.City;
import ru.marinin.schemas.Country;

/**
 * Created by prochiy on 29.07.18.
 */
@Service("Processing")
public class ProcessingImpl implements Processing {

    @Autowired
    SparkSession sparkSession;

    public void loadFiles(){

        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset df = sqlContext.read().csv("src/main/resources/GlobalTemperatures.csv");
        //df.show();

        Dataset dfCountry = sqlContext.read()
                .option("mode", "DROPMALFORMED")
                .schema(Country.schema)
                .csv("src/main/resources/GlobalLandTemperaturesByCountry.csv");
        dfCountry.show();

        dfCountry.createOrReplaceTempView("Country");
        Dataset<Row> sqlCountryYear = sparkSession.sql(
                "select SUBSTRING(dt , 1, 4) as YEAR, Country, AVG(AverageTemperature) as AverageT, " +
                        "MIN(AverageTemperature) as minT, Max(AverageTemperature) as maxT " +
                        "from Country " +
                        "group by YEAR, Country " +
                        "order by YEAR "

        );
        sqlCountryYear.createOrReplaceTempView("CountryYear");
        sqlCountryYear.show();

        Dataset<Row> sqlCountryDecadent = sparkSession.sql(
                "select SUBSTRING(YEAR , 1, 3) as DECADENT, Country, AVG(AverageT) as AverageT, " +
                        "MIN(minT) as minT, Max(maxT) as maxT " +
                        "from CountryYear " +
                        "group by DECADENT, Country " +
                        "order by DECADENT "

        );
        sqlCountryDecadent.createOrReplaceTempView("CountryDecadent");
        sqlCountryDecadent.show();

        Dataset<Row> dfCity = sqlContext
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(City.schema)
                .csv("src/main/resources/GlobalLandTemperaturesByCity.csv");
        dfCity.show();


        dfCity.createOrReplaceTempView("City");
        Dataset<Row> sqlSityYear = sparkSession.sql(
                "select SUBSTRING(dt , 1, 4) as YEAR, City, Country, AVG(AverageTemperature) as AverageT, " +
                 "MIN(AverageTemperature) as minT, Max(AverageTemperature) as maxT " +
                "from City " +
                "group by YEAR, City, Country " +
                "order by YEAR "

        );
        sqlSityYear.createOrReplaceTempView("CityYear");
        sqlSityYear.show();

        Dataset<Row> sqlSityDecadent = sparkSession.sql(
                "select SUBSTRING(YEAR , 1, 3) as DECADENT, City, Country, AVG(AverageT) as AverageT, " +
                        "MIN(minT) as minT, Max(maxT) as maxT " +
                        "from CityYear " +
                        "group by DECADENT, City, Country " +
                        "order by DECADENT "

        );

        sqlSityDecadent.createOrReplaceTempView("CityDecadent");
        sqlSityDecadent.show();

        //Здесть надо окуратно соединить все таблицы, чего я не успел сделать
        Dataset<Row> sqlResult = sparkSession.sql
                ("select CityYear.Year, CityYear.City, CityYear.Country, CityYear.AverageT, CityYear.minT, CityYear.maxT  " +
                 "from CityYear join CityDecadent on CityYear.City = CityDecadent.City " +
                                                " and CityYear.Country = CityDecadent.Country " +
                                                " and SUBSTRING(CityYear.Year, 1, 3) = CityDecadent.Decadent "
                );
        sqlResult.show();


        sqlResult.write().option("mode", "DROPMALFORMED").format("parquet").save("src/main/resources/result.parquent");

    }



}
