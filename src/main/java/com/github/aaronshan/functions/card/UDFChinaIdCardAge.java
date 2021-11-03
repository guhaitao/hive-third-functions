package com.github.aaronshan.functions.card;

import com.github.aaronshan.functions.utils.CardUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * age
 *
 * @author guhaitao
 * @create 2021-11-03-15:10
 */
@Description(name = "id_card_age"
        , value = "_FUNC_(string, string) - get age by given china id card and ds."
        , extended = "Example:\n > select _FUNC_(string, string) from src;")
public class UDFChinaIdCardAge extends UDF {
    private Text result = new Text();

    public UDFChinaIdCardAge() {
    }

    public Text evaluate(Text idCard, Text ds) throws ParseException {
        if (idCard == null || ds == null) {
            return null;
        }
        int birthday = CardUtils.getIdCardAge(idCard.toString(), ds.toString());
        if (birthday == -2) {
            return null;
        }
        result.set(String.valueOf(birthday));
        return result;
    }

    public Text evaluate(Text idCard) throws ParseException {
        if (idCard == null) {
            return null;
        }
        // 若未传日期，则自动使用当前时间作为日期
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String ds = df.format(new Date());
        int birthday = CardUtils.getIdCardAge(idCard.toString(), ds.toString());
        System.out.println(birthday);
        if (birthday == -2) {
            return null;
        }
        result.set(String.valueOf(birthday));
        return result;
    }

    public static void main(String[] args) throws ParseException {
        String idCard = "372927197901133326";
        UDFChinaIdCardAge udfChinaIdCardAge = new UDFChinaIdCardAge();
        System.out.println(udfChinaIdCardAge.evaluate(new Text(idCard)));
    }
}