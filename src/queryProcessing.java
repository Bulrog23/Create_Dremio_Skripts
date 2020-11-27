import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

//ersetzte alle FROM s als FROM "@dremio".tablename -->In joins müsste dann die Froms auch ersetzt werden

public class queryProcessing{
    public static void main(String args[]) throws FileNotFoundException {
        //String pathSql="/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark/Bimbo/queries/1.sql";
        String pathBenchmark="/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark";
        String speicherFolder="/media/jonas/TOSHIBA EXT/Jonas/Skripte/sqlQueries_bearbeitet_einzeln";
        queryPreprocessingLoop(pathBenchmark,speicherFolder);
        //getAllJoins(pathBenchmark,speicherFolder);
        //queryPreprocessing(pathSql);
    }
    public static String[] queryPreprocessing(String pathSql)throws FileNotFoundException {
        File text = new File(pathSql);
        Scanner scnr = new Scanner(text);
        String line = scnr.nextLine(); //erste Zeile string = hat nur eine Zeile
        //werden alle From replaced egal wie tabelle heißt
        String table = "";
        String[] arrayLine = line.split(" ");
        for(int i=0; i<arrayLine.length; i++){
            //System.out.println(arrayLine[i]);
            if(arrayLine[i].startsWith("FROM")){
                //System.out.println(arrayLine[i+1]);
                String[] tableName = arrayLine[i+1].split("\\.");
                table = tableName[0];
                if(table.startsWith("(cast")){
                    table=table.substring(6,tableName[0].length());
                }
                table = table.replace("\"", "");
                break;
                //wollte vllt alle Tabellennamen (in einer query mehr tabellen benutzt aber anscheinend eh nicht der Fall
                /*
                if(!tableNames.startsWith(table)&&table.length()>2){
                    tableNames+=table.substring(1,table.length()-1);
                }*/
            }
        }

        line = line.replace("FROM ","FROM \"@dremio\".");
        return new String[]{line, table};
        /* alle occurencies indexes of from
        int index = line.indexOf("FROM ");
        while (index >= 0) {
            System.out.println(index);
            index = line.indexOf("FROM ", index + 1);
        }
        */
        //String tableName = line.substring(line.indexOf("FROM")+6);
        //tableName = tableName.substring(0,line.indexOf("\"")); //z.B = Arade_1
        //String tableNamea = "\""+tableName+"\"";
        //String newTableName= "\"@dremio\"."+tableNamea;
        //line = line.replace(tableNamea,newTableName);
    }


    public static void queryPreprocessingLoop(String pathToBenchmarkFolder, String speicherFolder) throws FileNotFoundException {
        File folder = new File(pathToBenchmarkFolder);
        File[] listOfFiles = folder.listFiles();
        //HashMap<String, String> absolutePathDatenCSV = new HashMap<String, String>();
        String replacedQuery ="";
        String gesamtReplacedQuery="";
        int i = 1; //MedPayment2_1 == NR1   weiß nicht warum
        //BSP-Pfad: /media/jonas/TOSHIBA EXT/Jonas/testDaten/AirlineSentiment/AirlineSentiment_1.csv.bz2
        for (File file : listOfFiles) {
            System.out.println(file.getName());
            String path2 = pathToBenchmarkFolder + ("/" + file.getName()+"/queries");
            //file.getName()=Arade foldername
            File folder2 = new File(path2);
            File[] listOfFiles2 = folder2.listFiles();
            for (File file2 : listOfFiles2) {
                if (file2.getName().endsWith("sql")) { //wenn schon mal txt dateien erstellt wurden(aussortiert)
                    String absolutePathFile = file2.getAbsolutePath();
                    replacedQuery = queryPreprocessing(absolutePathFile)[0];
                    String tableNameFile = queryPreprocessing(absolutePathFile)[1];
                    //String speicherPathNewSQLQuery = speicherFolder+"/"+file.getName()+"_NR"+i+".sql";
                    String speicherPathNewSQLQuery = speicherFolder+"/"+tableNameFile+"_NR"+i+".sql";

                    gesamtReplacedQuery+=replacedQuery+"\n";
                    /*try {
                        Files.write(Paths.get(speicherPathNewSQLQuery), replacedQuery.getBytes()); //erstellt sql datei
                    } catch (IOException e) {
                        e.printStackTrace();
                    }*/
                    i++;
                }
            }
        }
        try {
            Files.write(Paths.get("/media/jonas/TOSHIBA EXT/Jonas/Skripte/allReplacedQuery.sql"), gesamtReplacedQuery.getBytes()); //erstellt sql datei
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Boolean jointest(String pathSql)throws FileNotFoundException {
        File text = new File(pathSql);
        Scanner scnr = new Scanner(text);
        String line = scnr.nextLine(); //erste Zeile string = hat nur eine Zeile
        return line.contains("JOIN")||line.contains("join");
    }

    public static void getAllJoins(String pathToBenchmarkFolder, String speicherFolder) throws FileNotFoundException {
        File folder = new File(pathToBenchmarkFolder);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            String path2 = pathToBenchmarkFolder + ("/" + file.getName()+"/queries");
            //file.getName()=Arade_1 foldername
            File folder2 = new File(path2);
            File[] listOfFiles2 = folder2.listFiles();
            for (File file2 : listOfFiles2) {
                if (file2.getName().endsWith("sql")) { //wenn schon mal txt dateien erstellt wurden(aussortiert)
                    String absolutePathFile = file2.getAbsolutePath();
                    if(jointest(absolutePathFile)){
                        System.out.println(absolutePathFile);
                    }
                }
            }
        }
    }
}