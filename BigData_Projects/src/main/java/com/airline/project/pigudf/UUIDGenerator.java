package com.airline.project.pigudf;

import java.io.IOException;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.Random;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class UUIDGenerator extends EvalFunc<String> {
    public String exec(Tuple input)
            throws IOException
    {
        try
        {
            Calendar calendar = Calendar.getInstance();

            String time = calendar.get(1) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(2) + 1) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(5)) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(11)) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(12)) }) + String.format("%1$02d", new Object[] {Integer.valueOf(calendar.get(13)) }) + String.format("%1$03d", new Object[] {Integer.valueOf(calendar.get(14)) });
            BigInteger randomNumber19digits = new BigInteger(63, new Random());
            String finalValue = time + String.format("%1$019d", new Object[] { randomNumber19digits });
            return finalValue;
        }
        catch(Exception e)
        {
// Throwing an exception will cause the task to fail.
            throw new IOException("Exception!!!!", e);
        }

    }

}