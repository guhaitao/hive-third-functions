package com.github.aaronshan.functions.string;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 全角转化半角函数
 *
 * @author guhaitao
 * @create 2021-11-01-14:42
 */
@Description(
        name = "full2half",
        value = "_FUNC_(full_string) - Convert full-width to half-width, or NULL if the argument was NULL.",
        extended = "Example:\n > select _FUNC_(full_string) from src;"
)
public class UDFFull2Half extends UDF {

    public UDFFull2Half() {
    }

    public String evaluate(String fullStr) throws Exception {
        if (StringUtils.isBlank(fullStr)) {
            return "";
        }

        StringBuilder outStrBuf = new StringBuilder("");
        String tStr = "";
        byte[] b = null;
        for (int i = 0; i < fullStr.length(); i++) {
            tStr = fullStr.substring(i, i + 1);
            // 全角空格转换成半角空格
            if (tStr.equals("　")) {
                outStrBuf.append(" ");
                continue;
            }

            b = tStr.getBytes("unicode");
            // 得到 unicode 字节数据
            if (b[2] == -1) { // 表示全角？
                b[3] = (byte) (b[3] + 32);
                b[2] = 0;
                outStrBuf.append(new String(b, "unicode"));
            } else {
                outStrBuf.append(tStr);
            }
        }
        // end for.
        return outStrBuf.toString();
    }

}