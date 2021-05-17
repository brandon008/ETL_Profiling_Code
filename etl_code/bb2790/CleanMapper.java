import java.io.*;
import java.lang.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String lines = value.toString();

        int count = 0;
        int flag = 0;
        String result = "";
        String mark = "mark";


        for(String line : lines.split("\n")){
            String[] array = line.split(",", -1);
            if(array.length > 2) {

                if(array[2].contains("493") || array[0].contains("  ") || array[0].contains("\n") || array[3].equals("")){ // stop point
                    flag = 1;
                }

                if(array[1].contains("Total") ) {
                    array[0] = "";
                    array[1] = ""; // cleaning random proceeding quote
                    array[2] = "";
                }
                array[1] = array[1].replace("\\1\\", ""); //cleaning strange \1\
                array[0] = array[0].replace("\\4\\", ""); //cleaning strange \4\
                array[1] = array[1].replace("$", "");
                if(array[2].contains("4-year")){  //removing strange treating - as another column
                    array[0] = "Selected Year";
                    array[1] = "Average tuition for all institutions";
                    array[2] = " ";

                }
                if(array[0].equals("1") && array[1].equals("2") && array[2].equals("3")){
                    array[0] = "";
                    array[1] = "";
                    array[2] = "";
                }
                if(!array[1].equals("") && !array[2].equals("")) {
                    count +=1;
                    if(flag == 1){
                            result += array[0] + "," + array[1] + array[2] + mark +"\n";
                    }else{
                            result += array[0] + "," + array[1] + array[2] + "\n";
                    }

                }
            }
        }

        for(String res : result.split("\n")){
            count += 1;

            if (res.length() == 0){
                    flag = 1;
                    break;
            }

            if (!res.contains("mark") && res.length() < 52){

            context.write(new Text(res), new Text(""));

            }

        }
        return;
    }
}
