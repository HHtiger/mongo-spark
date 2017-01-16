package com.fonuder.syrkxxzb;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by tiger on 2017/1/12.
 */
public class Insert {

    public static void main(String[] args) throws ParseException {

        SparkConf sc = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.executor.memory","14g")
                .set("spark.driver.memory","8g")
                .set("spark.driver.cores","2")
                .set("spark.mongodb.input.uri", "mongodb://172.29.214.23:27017/founder.syrkxxzb")
                .set("spark.mongodb.output.uri", "mongodb://172.29.214.23:27017/founder.syrkxxzb");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        Random r = new Random();
        List<Document> list = new ArrayList<>();

        int i = 0;

        for (int j = 0; j < 1000; j++) {
            for (int k = 0; k < 10000; k++) {
                Document d = new Document();
                d.append("_id", i);
                d.append("RYID", "RYID_" + i);
                d.append("XZQH", Arrays.asList(""+i,""+(i+1)));
                d.append("SYRKYWLXDM", "SYRKYWLXDM_" + i);
                d.append("SFYZ", "SFYZ_" + i);
                d.append("SFJN", "SFJN_" + i);
                d.append("CYZJDM", "CYZJDM_" + i);
                d.append("ZJHM", "ZJHM_" + i);
                d.append("WWX", "WWX_" + i);
                d.append("WWM", "WWM_" + i);
                d.append("XM", "XM_" + i);
                d.append("XBDM", "XBDM_" + i);
                d.append("MZDM", "MZDM_" + i);
                d.append("CSRQ", new Timestamp(System.currentTimeMillis() - 60 * 60 * 24 * 1000 * r.nextInt(1000)));
                d.append("CSRQEND", new Timestamp(System.currentTimeMillis() - 60 * 60 * 24 * 1000 * r.nextInt(1000)));
                d.append("CYM", "CYM_" + i);
                d.append("JGGJDQDM", "JGGJDQDM_" + i);
                d.append("GJDQDM", "GJDQDM_" + i);
                d.append("JGSSXDM", "JGSSXDM_" + i);
                d.append("HJD_XZQHDM", "HJD_XZQHDM_" + i);
                d.append("HJD_SJDM", "HJD_SJDM_" + i);
                d.append("HJD_FXJDM", "HJD_FXJDM_" + i);
                d.append("HJD_PCSDM", "HJD_PCSDM_" + i);
                d.append("HJD_SQDM", "HJD_SQDM_" + i);
                d.append("HJD_ZRQDM", "HJD_ZRQDM_" + i);
                d.append("HJD_DZID", "HJD_DZID_" + i);
                d.append("HJD_DZXZ", "HJD_DZXZ_" + i);
                d.append("HJD_ZBX", "HJD_ZBX_" + i);
                d.append("HJD_ZBY", "HJD_ZBY_" + i);
                d.append("GXSJDM", "GXSJDM_" + i);
                d.append("GXFJDM", "GXFJDM_" + i);
                d.append("GXPCSDM", "GXPCSDM_" + i);
                d.append("GXSQDM", "GXSQDM_" + i);
                d.append("GXZRQDM", "GXZRQDM_" + i);
                d.append("RYSX", "RYSX_" + i);
                d.append("PYSZM", "PYSZM_" + i);
                d.append("WBSZM", "WBSZM_" + i);
                d.append("SECURITYGRADE", "SECURITYGRADE_" + i);
                d.append("SJLY", "SJLY_" + i);
                d.append("KYJX", "KYJX_" + i);
                d.append("KYXW", "KYXW_" + i);
                d.append("BYYX", "BYYX_" + i);
                d.append("HH", "HH_" + i);
                d.append("HKLB", "HKLB_" + i);
                d.append("SH", "SH_" + i);
                d.append("JL", "JL_" + i);
                d.append("JKZK", "JKZK_" + i);
                d.append("YHZGX", "YHZGX_" + i);
                d.append("ZWBH", "ZWBH_" + i);
                d.append("BZ", "BZ_" + i);
                d.append("XT_CJSJ", "XT_CJSJ_" + i);
                d.append("XT_LRSJ", "XT_LRSJ_" + i);
                d.append("XT_LRRXM", "XT_LRRXM_" + i);
                d.append("XT_LRRID", "XT_LRRID_" + i);
                d.append("XT_LRRBM", "XT_LRRBM_" + i);
                d.append("XT_LRRBMID", "XT_LRRBMID_" + i);
                d.append("XT_LRIP", "XT_LRIP_" + i);
                d.append("XT_ZHXGSJ", "XT_ZHXGSJ_" + i);
                d.append("XT_ZHXGRXM", "XT_ZHXGRXM_" + i);
                d.append("XT_ZHXGRID", "XT_ZHXGRID_" + i);
                d.append("XT_ZHXGRBM", "XT_ZHXGRBM_" + i);
                d.append("XT_ZHXGRBMID", "XT_ZHXGRBMID_" + i);
                d.append("XT_ZHXGIP", "XT_ZHXGIP_" + i);
                d.append("XT_ZXBZ", "XT_ZXBZ_" + i);
                d.append("XT_ZXYY", "XT_ZXYY_" + i);
                d.append("XT_ZXYYBZ", "XT_ZXYYBZ_" + i);
                d.append("OLD_ID", "OLD_ID_" + i);
                d.append("OLD_RYID", "OLD_RYID_" + i);
                d.append("SSDQ", "SSDQ_" + i);
                list.add(d);
                i++;
            }
            JavaRDD<Document> javaRDD = jsc.parallelize(list);
            MongoSpark.save(javaRDD);
            list.clear();
        }

    }

}
