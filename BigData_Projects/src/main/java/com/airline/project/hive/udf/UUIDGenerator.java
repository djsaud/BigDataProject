package com.airline.project.hive.udf;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.Random;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic=false)
@Description(name="uuid", value="uuid() - returns a 36 digit UUID", extended="Example:\n > SELECT uuid(),* FROM tablename;")
public class UUIDGenerator
        extends UDF
{
    public Text evaluate()
    {
        Calendar calendar = Calendar.getInstance();

        String time = calendar.get(1) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(2) + 1) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(5)) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(11)) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(12)) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(13)) }) + String.format("%1$03d", new Object[] {Integer.valueOf(calendar.get(14)) });
        BigInteger randomNumber19digits = new BigInteger(63, new Random());
        String finalValue = time + String.format("%1$019d", new Object[] { randomNumber19digits });
        return new Text(finalValue);
    }
}