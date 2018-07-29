package ru.marinin;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by prochiy on 08.07.17.
 */
public class Main {


    public static void main(String[] args){

        ApplicationContext ap = new AnnotationConfigApplicationContext(LocalConfig.class);
        String test = (String)ap.getBean("testConfig");
        System.out.println(test);

        Processing proc = ap.getBean("Processing", ProcessingImpl.class);


        proc.loadFiles();

    }
}
