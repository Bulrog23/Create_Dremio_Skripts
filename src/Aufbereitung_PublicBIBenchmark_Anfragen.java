import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

//Die Methoden dienen nur zur Überarbeitung der SQL-Anfragen-> senden oder benutzten der Anfragen muss über die Schnittstellen erfolgen
//die erstellten SQL-Anfragen können über jede Schnittstelle an Dremio gesendet werden (Anfragen funktionieren getesetet in REST, UI oder DBeaver)

//ersetzte alle FROM s als FROM "@dremio".tablename (Da Dremio den dremio Path zu den Datasets benötigt)
//"@dremio". = dremioFolderPathName (HauptOrdner = username)

//Methode funktioniert nur für Public Bi Benchmark bis jetzt
//funktioniert noch nicht mit joins mit mehreren Tables -> nur 1 TableNamePath richtig mit @dremio ersetzt
//in Public Bi Daten exe. sowas nicht (immer max. ein Table benutzt)

//Beispiel-Ergebnis: SELECT "Arade_1"."F4" AS "F4" FROM "@dremio"."Arade_1" WHERE ((CAST("Arade_1"."F3" as DATE) >= cast('2014-10-17' as DATE)) AND (CAST("Arade_1"."F3" as DATE) <= cast('2015-10-16' as DATE))) GROUP BY "F4" LIMIT 130;

public class Aufbereitung_PublicBIBenchmark_Anfragen {
    public static void main(String args[]) throws FileNotFoundException {

        //Path von Public Bi Benchmark (nötig um die table dateien auszulesen)
        String pathBenchmark="/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark";
        //in Abgabe ist der Path ~/Bachelor/public_bi_benchmark-master/benchmark

        //Speicherort für die erstellten Skripte/Anfragen
        String speicherFolderForAll = "/media/jonas/TOSHIBA EXT/Jonas/Skripte";

        String zielOrdnerDremio = "\"@dremio\""; //Path der Datasets in Dremio (in Tests -> localHostServer-Hauptordner, Hauptordner==Username)

        queryPreprocessingLoop(pathBenchmark, zielOrdnerDremio, speicherFolderForAll);

        //Alte-Hilfsmethode zum herrausfinden von Joins
        //getAllJoins(pathBenchmark,speicherFolderEinzeln);
        //queryPreprocessing(pathSql);
    }

    //in dieser Methode werden SQL-Anfragen für Dremio überarbeitet:
    // es werden alle FROMs ersetzt -> da in den FROM der genaue Dremio Path der Daten stehen muss
    //Beispiel FROM Arade_1 --> FROM "@dremio\"."Arade_1 (Beispiel: CAST("CityMaxCapita_1"."Number of Records" AS BIGINT))

    // es wird nach CASTs der Dataset Name angegeben

    // Es können mehreren FROMs vorkommen -> werden hier erfasst
    //ABER: werden mit dieser nur erfasst, da diese den selben Namen haben -> in public bi Benchmark, arbeitet jede SQL-Anfrage höchstens mit einen Dataset
    //falls die Methode für andere Anfragen benutzt werden soll -> überarbeiten
    public static String[] queryPreprocessing(String pathSql, String zielOrdner)throws FileNotFoundException {
        File text = new File(pathSql);
        Scanner scnr = new Scanner(text);
        String line = scnr.nextLine(); //erste Zeile überspringen
        String table = "";
        String[] arrayLine = line.split(" "); //alle Wörter werden in ein Array geschoben
        for(int i=0; i<arrayLine.length; i++){ //Array wird durch iteratiert
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

//loopt durch den public bi Benchmark Ordner durch -> erstellt eine Datei mit allem überarbeiteten SQL-Anfragen
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

    /*
    //Hilfsmethoden um alle Joins im public bi benchmark zu finden (Ergebnis = es exe. weniger Joins als gedacht)

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
    }*/
}