import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

//ersetzte alle FROM s als FROM "@dremio".tablename -->In joins müsste dann die Froms auch ersetzt werden
//"@dremio". = dremioFolderPathName (HauptOrdner = username)

//funktioniert noch nicht mit joins mit mehrern Tables -> nur 1 TableNamePath richtig mit @dremio ersetzt
//in Testdaten exe. sowas nicht

public class queryProcessing{
    public static void main(String args[]) throws FileNotFoundException {
        //String pathSql="/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark/Bimbo/queries/1.sql";
        //Path für table dateien
        String pathBenchmark="/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark";

        //für Dirk Server
        String speicherFolderForAll = "/media/jonas/TOSHIBA EXT/Jonas/SkripteDirkServer";

        //für local Server
        //String speicherFolderForAll = "/media/jonas/TOSHIBA EXT/Jonas/Skripte;

        //String zielOrdnerDremio = "\"@dremio\""; //localHostServer, Hauptordner=Username
        String zielOrdnerDremio = "\"@greim\""; //dirk Server

        queryPreprocessingLoop(pathBenchmark, zielOrdnerDremio, speicherFolderForAll);
        //getAllJoins(pathBenchmark,speicherFolderEinzeln);
        //queryPreprocessing(pathSql);
    }
    public static String[] queryPreprocessing(String pathSql, String zielOrdner)throws FileNotFoundException {
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

        //line = line.replace("FROM ","FROM \"@dremio\".");;
        String replace = "FROM "+zielOrdner+".";
        line = line.replace("FROM ",replace);

        return new String[]{line, table};
    }


    public static void queryPreprocessingLoop(String pathToBenchmarkFolder, String zielOrdnerDremio, String speicherFolderForAll) throws FileNotFoundException {
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
                    replacedQuery = queryPreprocessing(absolutePathFile, zielOrdnerDremio)[0];
                    String tableNameFile = queryPreprocessing(absolutePathFile, zielOrdnerDremio)[1];
                    //String speicherPathNewSQLQuery = speicherFolder+"/"+file.getName()+"_NR"+i+".sql";
                    String grundPath = speicherFolderForAll+"/sqlQueries_bearbeitet_einzeln";
                    File fileA = new File(grundPath);
                    boolean dirCreated = fileA.mkdir(); //created alle Ordner des Pathes die noch nicht exe.
                    String speicherPathNewSQLQuery = grundPath+"/"+tableNameFile+"_NR"+i+".sql";

                    gesamtReplacedQuery+=replacedQuery+"\n";
                    try {
                        Files.write(Paths.get(speicherPathNewSQLQuery), replacedQuery.getBytes()); //erstellt sql datei
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    i++;
                }
            }
        }
        String speicherPath = speicherFolderForAll+"/allReplacedQuery.sql";
        try {
            Files.write(Paths.get(speicherPath), gesamtReplacedQuery.getBytes()); //erstellt sql datei
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