package com.hosuke.mapreduce.kmeans;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author Hosuke
 */
public class CreateTrainingDataSet  {

    private static void writePoint(FileWriter fw) throws IOException {
        float x = new Random().nextFloat() * 10000;
        float y = new Random().nextFloat() * 10000;
        String lineRecordString = String.valueOf(x) + "," + String.valueOf(y) + "\r\n";
        fw.write(lineRecordString);
    }

    private static void CreateDataSet()
    {
        int count=1;
        try {

            FileWriter fw1 = new FileWriter("initK");
            while(count<=10) {
                writePoint(fw1);
                count++;
            }
            fw1.close();

            count=1;
            FileWriter fw2 = new FileWriter("kmeans");
            while(count<=6000000) {
                writePoint(fw2);
                count++;
            }
            fw2.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        CreateDataSet();
    }


}