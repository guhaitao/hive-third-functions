package com.github.aaronshan.functions.string;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * 处理hive中utf8字符串中的特殊表情符号,mysql导入数据时,提示非法字符,
 * 解决方案:
 * 1.可通过修改 mysql的编码集为: utf8mb4,缺点:需要重启数据库,线上环境不友好
 * 2.通过自定义函数将非法字段过滤,替换为空或其他自定义字符串,缺点: 需要编码,将jar上传.
 *
 * @author guhaitao
 * @create 2021-11-02-14:42
 */

@Description(name = "filter_illegal_char"
        , value = "_FUNC_(string) - Filter illegal characters, or NULL if the argument was NULL."
        , extended = "Example:\n > select _FUNC_(string) from src;")
public class UDFFilterIllegalChar extends UDF {

    private Text result = new Text();

    public UDFFilterIllegalChar() {
    }

    public Text evaluate(Text fullStr) {
        if (fullStr == null) {
            return null;
        }

        result.set(fullStr.toString().replaceAll("[\\x00-\\x08]|[\\x0A-\\x1F]|\\x7F|[\\p{Cf}]", ""));
        return result;
    }

    public Text evaluate(BytesWritable fullStr) {
        if (fullStr == null) {
            return null;
        }

        result.set(fullStr.toString().replaceAll("[\\x00-\\x08]|[\\x0A-\\x1F]|\\x7F|[\\p{Cf}]", ""));
        return result;
    }

}
