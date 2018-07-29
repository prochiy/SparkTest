package ru.marinin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.jmx.export.annotation.AnnotationMBeanExporter;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * Created by prochiy on 08.07.17.
 */

//@ImportResource("classpath:applicationContext.xml")
@ComponentScan(basePackages = {"ru.marinin"})
@Configuration
public class LocalConfig {

    @Bean("testConfig")
    public String testConfig(){
        return "testConfig";
    }

    @Bean
    public AnnotationMBeanExporter annotationMBeanExporter() {
        return new AnnotationMBeanExporter();
    }


    @Bean
    public SparkConf sparkConf(){
        SparkConf conf = new SparkConf();
        conf.setAppName("spark application");
        conf.setMaster("local[4]");
        return conf;
    }

    //@Bean
    //public JavaSparkContext javaSparkContext(){
    //    return new JavaSparkContext(sparkConf());
    //}

    @Bean
    public SparkSession sparkSession(){
        return SparkSession.builder()
                .config(sparkConf())
                .getOrCreate();
    }

}
