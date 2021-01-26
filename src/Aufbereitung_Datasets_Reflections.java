import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

//Erstellt zu jeden Dataset eine SQL-Anfragen für:
    //Aggregation-Reflection Erstellung
    //RAW-Reflection Erstellung
    //Virtuell Dataset Erstellung (mit denen die Spaltendatentypen&Spaltennamen der CSV daten gesetzt werden)


//Diese Klasse erstellt die SQL-Anfragen mit denen die Spaltendatentypen und Spaltennamen der CSV-Dateien in Dremio gesetzt werden
//+Erstellt SQL-Anfragen für die Reflection-Erstellung
//Es wird mit der SQL-Anfrage ein VDS(Virtual Dataset) erstellt indem alle Typen&Namen in Dremio gesetzt werden
//dadurch müsssen CSV Dateien nicht aufwendig in der UI manuell aufbereitet werden (manuell Typen und Namen setzten --> sehr Zeit intensiv)
//Die CSV-Dateien können nicht unaufbereitet die Anfragen des Public bi Benchmarks ausführen
//Obwohl die Public bi Benchmark Anfragen Casts und As ausführen -> viele Datentypen erkennt Dremio nicht, setzt sie falsch oder erkennt Nulls nicht
//wenn die Typen&Namen richtig gesetzt sind  -> kann man die public bi Benchmark Anfragen ausführen
//Die SQL-Anfragen des Public Benchmarks werden in der Klasse Aufbereitung_PublicBIBenchmark_Anfragen umgewandelt

//Beispiele:
//RAW-Erstellung-SQL:           ALTER DATASET "@dremio"."Arade_1" CREATE RAW REFLECTION Arade_1_RAW USING DISPLAY ("F1","F2","F3","F4","F5","F6","F7","F8","F9","Number of Records","WNET (bin)");
//Aggregation-Erstellung-SQL:   ALTER DATASET "@dremio"."Arade_1" CREATE AGGREGATE REFLECTION Arade_1_AGG USING DIMENSIONS ("F1","F2","F3","F6","F7") MEASURES ("F4","F5","F8","F9","Number of Records","WNET (bin)");
//CSV_Aufbereitung-SQL:         CREATE VDS "@dremio".Arade_1 as SELECT CAST("A1" AS VARCHAR) as "F1", CAST("B1" AS VARCHAR) as "F2", TO_TIMESTAMP((SUBSTR("C", 0, LENGTH("C") - 3)), 'YYYY-MM-DD HH24:MI:SS.FFF', 1) as "F3", CONVERT_TO_FLOAT("D", 1, 1, 0) as "F4", CONVERT_TO_FLOAT("E", 1, 1, 0) as "F5", CAST("F1" AS VARCHAR) as "F6", CAST("G1" AS VARCHAR) as "F7", CONVERT_TO_FLOAT("H", 1, 1, 0) as "F8", CONVERT_TO_FLOAT("I", 1, 1, 0) as "F9", CONVERT_TO_INTEGER("J", 1, 1, 0) as "Number of Records", CONVERT_TO_INTEGER("K", 1, 1, 0) as "WNET (bin)" FROM (SELECT *,  CASE WHEN "A" = 'null' THEN NULL ELSE "A" END AS "A1", CASE WHEN "B" = 'null' THEN NULL ELSE "B" END AS "B1", CASE WHEN "F" = 'null' THEN NULL ELSE "F" END AS "F1", CASE WHEN "G" = 'null' THEN NULL ELSE "G" END AS "G1" FROM allData.Arade."Arade_1.csv.bz2");


//Genauere Funktionserklärung: (wird auf allen Table Dateien im Public Bi Benchmark durch iteriert)

//Ausgangssituation:
//Physical Datasets (PDS) liegen in NAS-Ordner in Dremio (ohne Spaltennamen und Spaltentyp)
//in PDS ist defaultmäßig der Spaltentyp VARCHAR und die Spaltennamen sind nach dem Alphabet benannt (A,B,C,...)

//Es wird eine Anfrage generiert die aus dem PDS eine Virtual Dataset (VDS) erstellt und auf diesen Virtual Datasets Anfragen anlagern, die auto. typen&Namen setzten

//Table-Dateien werden aus Benchmark-Ordner ausgelesen --> dann wird jedes Wort der Table -Datei in einem Feld einens Array dargstellt (->auslesbar)
//Spalten-Datentypen:
//-> aus dem Array werden die Datentypen extrahiert
//-> Datentypen werden in die Dremio konformen Datentypen umgewandelt
//-> Je nach Dremio-Datentyp werden die passenden Casts in der Ergebniss-Anfrage eingefügt (dadruch dass default Spaltenbenennung nach ABC benennt -> wird die richtige Spalten ausgewählt)
//(es exe. Ausnahmen in den Spalten mit dem Typ Varchar und Boolean werden durch die Cast keine NULL werte gesetzt-> deshalb wird die Spalte komplett durch iteriert(mit CASE) und alle nulls werden als NULL gesetzt (deshalb A1 da die Spalte kurzfristig umbenannt werden muss)
//Spalten-Namen:
//Spaltennamen werden ausgelesen
//durch die regelmäßige default Spaltenbenennung werden in der Ergebnis-SQL-Anfrage die Spaltennamen richtig  mit AS gesetzt (da Spaltennamen Berechenbar sind)

//RAW Reflection Erstellung
//Spaltennamen aus Table-Datei ausgelesen
//es wird eine SQL Anfrage erstellt, die eine RAW-Reflection von dem gewünschten Dataset erstellt in der alle Spalten aggriert werden

//Aggregation Reflection Erstellung
//Spaltennamen und Typ aus Table-Datei ausgelesen
//je Nach Typ wird entweder eine Dimension oder ein Measure aggregiert
//hier: alle INT, Float oder Double Spalten -> Measure aggregiert
//      alle Anderen -> Dimension aggregiert



public class Aufbereitung_Datasets_Reflections {
    public static void main(String args[]) throws FileNotFoundException {
        String benchmarkOrdnerPath="/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark"; // Public BI Benchmark Path
        String datenOrdnerPath ="/media/jonas/TOSHIBA EXT/Jonas/daten/PublicBIbenchmark"; //Path des Ordner indem die CSV-Dateien sind
        String nas = "allData"; //NAS Ordner in Dremio (NAS wurde genutzt um die CSV-Daten in das Single-Node-Cluster zu laden
        //->Work-around da Dremio keine Multi-uploads ermöglicht und die UI ein upload Limit von 500MB hat

        String speicherOrtForAll = "/media/jonas/TOSHIBA EXT/Jonas/Skripte"; //Speicherort für alle erstellten SQL-Anfragen
        String zielOrdnerDremio = "\"@dremio\""; //localHostServer, Hauptordner=Username ->DateiName auto erstellt
        //pathNamen immer so angeben \"abc\" und pathNamen in Dremio getrennt durch Punkte

        HashMap<String, String> typeConvertDremio = new HashMap<String, String>();
        HashMap<String, String> absolutePathDatenCSVMapping = new HashMap<String, String>();
        mapping(typeConvertDremio); //erstellt Datentyp mapWerte für Dremio
        absolutePathDatenMapping(absolutePathDatenCSVMapping, datenOrdnerPath);
        //System.out.println(getAbsolutePathDaten(absolutePathDatenCSVMapping, "Arade_1.csv.bz2"));

        //Uwandlung aller Table im Ordner Benchmark
        process2txtLoop(typeConvertDremio, benchmarkOrdnerPath, absolutePathDatenCSVMapping, nas, speicherOrtForAll, zielOrdnerDremio);
    }

    //diese Methode erstellt ein Mapping wie Dremio die Datentypen intern mapped
    //https://docs.dremio.com/sql-reference/data-types-mysql.html
    public static void mapping(HashMap<String, String> typeConvertDremio){
        typeConvertDremio.put("bit","boolean");
        typeConvertDremio.put("boolean","boolean");
        typeConvertDremio.put("bigint","integer");
        typeConvertDremio.put("bigint unsigned","integer");
        typeConvertDremio.put("binary","varbinary");
        typeConvertDremio.put("blob","varbinary");
        typeConvertDremio.put("char","varchar");
        typeConvertDremio.put("date","date");
        typeConvertDremio.put("datetime","timestamp");
        typeConvertDremio.put("decimal unsigned","float");
        typeConvertDremio.put("decimal","float");
        typeConvertDremio.put("double","float");
        typeConvertDremio.put("double precision","float");
        typeConvertDremio.put("enum","varchar");
        typeConvertDremio.put("int","integer");
        typeConvertDremio.put("int unsigned","integer");
        typeConvertDremio.put("integer","integer");
        typeConvertDremio.put("long varbinary","varbinary");
        typeConvertDremio.put("long varchar","varchar");
        typeConvertDremio.put("longblob","varbinary");
        typeConvertDremio.put("longtext","varchar");
        typeConvertDremio.put("mediumblob","varbinary");
        typeConvertDremio.put("mediumint","integer");
        typeConvertDremio.put("mediumint unsigned","integer");
        typeConvertDremio.put("mediumtext","varchar");
        typeConvertDremio.put("numeric","float");
        typeConvertDremio.put("real","float");
        typeConvertDremio.put("set","varchar");
        typeConvertDremio.put("smallint","integer");
        typeConvertDremio.put("smallint unsigned","integer");
        typeConvertDremio.put("text","varchar");
        typeConvertDremio.put("time","time");
        typeConvertDremio.put("timestamp","timestamp");
        typeConvertDremio.put("tinyblob","varbinary");
        typeConvertDremio.put("tinyint","integer");
        typeConvertDremio.put("tinyint unsigned","integer");
        typeConvertDremio.put("tinytext","varchar");
        typeConvertDremio.put("varbinary","varbinary");
        typeConvertDremio.put("varchar","varchar");
        typeConvertDremio.put("year","integer");
        typeConvertDremio.put("year!","year!");//Ausnahme year in Reflecions (=Dimension)
    }

    public static String getDremio(HashMap<String, String> typeConvertDremio, String value){
        return typeConvertDremio.getOrDefault(value, " ERROR1:_TYP_NOT_MAPPED ");
    }


    public static String toText(HashMap<String, String> typeConvertDremio, String tablePath, HashMap<String, String> absolutePathDatenCSVMapping, String nas, String speicherOrtForAll, String zielOrdnerDremio) throws FileNotFoundException {
        File text = new File(tablePath);
        Scanner scnr = new Scanner(text);
        //String result = "CREATE VDS \"@dremio\"."+tablePath.substring(tablePath.lastIndexOf('/')+1, (tablePath.length()-10))+" as SELECT";
        String result = "CREATE VDS "+zielOrdnerDremio+"."+tablePath.substring(tablePath.lastIndexOf('/')+1, (tablePath.length()-10))+" as SELECT";
        //wenn ohne VDS Erstellung
        //String result = "SELECT";
        String booleanNull = "(SELECT *, ";
        int i = 64;
        scnr.nextLine(); //überspringt die erste Zeile
        while (scnr.hasNextLine()) { //schneidet aus jeder zeile Column-name raus
            String line = scnr.nextLine();
            //System.out.println(line);
            if (line.length() > 2) { //letzte zeile nicht lesen(ist nur klammer zu "};")
                i++; //65==A
                String lineCuttet = line.substring((line.lastIndexOf('"')+2),line.length()); //setzt den Datentyp in Liste auf Platz 0
                List<String> myList = new ArrayList<String>(Arrays.asList(lineCuttet.split(" "))); //wird abgeschnitten, da manche column names spaces enthalten
                //SELECT (CAST("A" as bigint) as newColumn,
                //SELECT CONVERT_TO_INTEGER("A", 1, 1, 0) AS "Agencia_ID"
                String ohneKommaZumSchluss = myList.get(0).replace(",", "");//Zeile kann quasi: ("A" double,) <-

                //Ausnahme: in decimal kein leerzeichen -> mehrere Wörter in einen Feld = decimal(7 oder decimal(7),
                if(ohneKommaZumSchluss.startsWith("decimal")){
                    ohneKommaZumSchluss = "decimal";
                }
                //Ausnhame: long varbinary => Datentyp aus 2 Wörtern
                //("long varbinary","varbinary"); ("long varchar","varchar");
                if(ohneKommaZumSchluss.startsWith("long")){
                    if(myList.get(0).startsWith("varb")){
                        ohneKommaZumSchluss = "varbinary";
                        ohneKommaZumSchluss = "varchar";
                    }
                }

                //Ausnahme: nach Varchar wird öfter die max. Länge angeben
                if(ohneKommaZumSchluss.startsWith("varchar")){
                    ohneKommaZumSchluss = "varchar";
                }

                //Datentyp wird hier zu dremio-daten-typen umgemappet
                ohneKommaZumSchluss = getDremio(typeConvertDremio, ohneKommaZumSchluss);

                //je nach Dremio-Datentyp werden die Internen Casts-Befehle angewandt/eingefügt
                //in Dokumentation exe. auch einfachere/einheitlichere Befehle -> funktionieren zum Teil nicht & erkennen keine NULLs (setzten es als String)
                switch(ohneKommaZumSchluss){
                    case "timestamp": result+=" TO_TIMESTAMP((SUBSTR("+getDefaultColumnNames(i)+", 0, LENGTH("+getDefaultColumnNames(i)+") - 3)), 'YYYY-MM-DD HH24:MI:SS.FFF', 1)";
                        //Klammer zu und as Columnname am ende
                        break;
                    case "float": result+=" CONVERT_TO_FLOAT("+getDefaultColumnNames(i)+", 1, 1, 0)";
                        break;
                    case "integer": result+=" CONVERT_TO_INTEGER("+getDefaultColumnNames(i)+", 1, 1, 0)";
                        break;
                    case "date": result+= " TO_DATE("+getDefaultColumnNames(i)+", \'YYYY-MM-DD\', 1)";
                        break;
                    case "time": result+= " TO_TIME("+getDefaultColumnNames(i) +", \'HH24:MI:SS.FFF\', 1)";
                        break;
                    // Option: do nothhing by varchar:
                    //case "varchar": result+= " "+getDefaultColumnNames(i); //nur column name umbenennen, default typ
                    //    break;
                    case "varbinary": result+="CONVERT_TO("+getDefaultColumnNames(i)+" \'UTF8\')";
                        break;

                        //Case wird in Anfrage eingearbeitet da Varchar und Boolean keine Null werte setzten -> Spalte durch iteriert mit Case und setzt alle nulls zu NULL
                    case "boolean":
                        String booleanColumnName = getDefaultColumnNames(i).substring(0,getDefaultColumnNames(i).length()-1)+"1\"";
                        result+= "CAST("+booleanColumnName+" AS BOOLEAN)";
                        booleanNull += " CASE WHEN "+getDefaultColumnNames(i)+" = \'null\' THEN NULL ELSE "+getDefaultColumnNames(i)+" END AS "+booleanColumnName+",";
                        break;
                    case "varchar":
                        String varcharColumnName = getDefaultColumnNames(i).substring(0,getDefaultColumnNames(i).length()-1)+"1\"";
                        result+= " CAST("+varcharColumnName+" AS VARCHAR)";
                        booleanNull += " CASE WHEN "+getDefaultColumnNames(i)+" = \'null\' THEN NULL ELSE "+getDefaultColumnNames(i)+" END AS "+varcharColumnName+",";
                        break;

                    /*Bsp.  FROM (
                            SELECT *,
                            CASE
                            WHEN "BN" = 'null' THEN NULL
                            Else "BN"
                            END AS "BN1", //als "BN1" umbenannt um beim oberen Select verwendet werden zu können
                            CASE
                            WHEN "BO" = 'null' THEN NULL
                            Else "BO"
                            END AS "BO2"
                            //Ab hier dieser Part wird am Ende bei From erzeugt =>geprüft ob ein boolean in Tabelle wenn ja dann erzeugt er ende
                            //wird erst bei From eingefügt, da dann sicher alle Boolean Prüfungen vorher eingefügt=>weiß nicht wann ende
                            FROM "@greim"."Uberlandia_1.csv")
                            */
                    default: result+= " Not_expected_Data_Typ(inCases) ";
                        break;
                }
                result += " as ";
                result += line.substring(2, (line.lastIndexOf('"')+1)); //Columnname with ""
                result += ",";
                //SELECT (CAST(A as bigint) as newColumn,
            }
        }
        result= result.substring(0,result.length()-1);//entfernt letztes Komma ,
        result+=" FROM ";
        if(booleanNull.contains("CASE")){
            booleanNull= booleanNull.substring(0,booleanNull.length()-1);//letztes Komma entfernen
            result += booleanNull;
            result += " FROM ";

            /*
            result+= "\"@greim\".\""; //Dremio-Server-UserName
            result+= tablePath.substring(tablePath.lastIndexOf("/")+1,tablePath.length()-10); //added TableName without.sql
            result+= ".csv\")";
            */
            String fileName = tablePath.substring(tablePath.lastIndexOf('/')+1, (tablePath.length()-10))+".csv.bz2";
            result += nas+"."; //PfadDremioNAS
            //nicht tablepath sonder path zur datei
            //muss jezz dateiname mit mapping aufrufen --> Path
            String absolutePathCSV = getAbsolutePathDaten(absolutePathDatenCSVMapping,fileName);
            String dateiName = absolutePathCSV.substring(absolutePathCSV.lastIndexOf("/")+1);
            String letzterQuer= absolutePathCSV.substring(0, absolutePathCSV.lastIndexOf("/"));
            String folderName= letzterQuer.substring(letzterQuer.lastIndexOf("/")+1);
            result += folderName+".\""+dateiName+"\");";
        }
        else {
            String fileName = tablePath.substring(tablePath.lastIndexOf('/')+1, (tablePath.length()-10))+".csv.bz2";
            result += nas+"."; //PfadDremioNAS
            //nicht tablepath sonder path zur datei
            //muss jezz dateiname mit mapping aufrufen --> Path
            String absolutePathCSV = getAbsolutePathDaten(absolutePathDatenCSVMapping,fileName);
            String dateiName = absolutePathCSV.substring(absolutePathCSV.lastIndexOf("/")+1);
            String letzterQuer= absolutePathCSV.substring(0, absolutePathCSV.lastIndexOf("/"));
            String folderName= letzterQuer.substring(letzterQuer.lastIndexOf("/")+1);
            result += folderName+".\""+dateiName+"\";";
            /*
            result += "\"@greim\".\""; //Dremio-Server-UserName
            result += tablePath.substring(tablePath.lastIndexOf("/") + 1, tablePath.length() - 10); //added TableName without.sql
            result += ".csv\"";
            */
        }

        //System.out.println(result);
        //Bsp. /Uberlandia_1.table_processed.sql
        String grundPath = speicherOrtForAll+"/VDS_TypNameSetter_einzeln";
        File file = new File(grundPath);
        boolean dirCreated = file.mkdir(); //created alle Ordner des Pathes die noch nicht exe.
        String newFilename = grundPath + tablePath.substring(tablePath.lastIndexOf('/'), (tablePath.length()-4)) + "_processed.sql"; //trennt .sql ab und hängt processed.sql an
        //String path1 = "C:/Users/jonas/Downloads/test99.sql"; //feste Speicheradresse für neue Datei
        //System.out.println(newFilename);

        //Speichern als sql dateien
        try {
            Files.write(Paths.get(newFilename), result.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    public static String getDefaultColumnNames(int i){
        //ASCII 65=A, 90=Z 91=AA 92=AB -->Über 90 2ter Char&(i-26)
        String ergebnis = "";
        int anzahl = 64;
        while(i>90){
            i = i-26; //alphabte hat 26 Buchstaben
            //wie oft Alphabet durchgespielt
            anzahl = anzahl+1;
        }
        if(anzahl>64){ //dann erste Digit adden
            ergebnis+=Character.toString ((char) anzahl);
        }
        ergebnis+=Character.toString ((char) i); //zweite oder erste digit adden
        ergebnis="\""+ergebnis+"\""; //in "" da AS & AT probleme als Befehle gelesen werden
        return ergebnis;
    }
    public static void process2txtLoop(HashMap<String, String> typeConvertDremio, String pathBenchmarkFolder, HashMap<String, String> absolutePathDatenCSVMapping, String nas, String speicherOrtForAll, String zielOrdnerDremio){
        File folder = new File(pathBenchmarkFolder);
        File[] listOfFiles = folder.listFiles();
        //createt den path zu jeder table.sql datei
        String allCommands ="";
        String allCommandsAggReflection="";
        String allCommandsRawReflection="";
        for (File file : listOfFiles) {
            String path2 = pathBenchmarkFolder + ("/" + file.getName() + "/tables");
            //System.out.println(path2);
            File folder2 = new File(path2);
            File[] listOfFiles2 = folder2.listFiles();
            for (File file2 : listOfFiles2) {
                if(file2.getName().endsWith("sql")) { //wenn schon mal txt dateien erstellt wurden(aussortiert)
                    String path3 = path2 + "/" + file2.getName();
                    try {
                        allCommands+=toText(typeConvertDremio, path3, absolutePathDatenCSVMapping, nas, speicherOrtForAll, zielOrdnerDremio)+"\n";
                        allCommandsAggReflection+=createReflections(typeConvertDremio,path3, speicherOrtForAll, zielOrdnerDremio)+"\n";
                        allCommandsRawReflection+=createRAWReflections(path3,speicherOrtForAll, zielOrdnerDremio)+"\n";
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        try {
            Files.write(Paths.get(speicherOrtForAll+"/allAggReflections.sql"), allCommandsAggReflection.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Files.write(Paths.get(speicherOrtForAll+"/all_VDS_SetterTypName.sql"), allCommands.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Files.write(Paths.get(speicherOrtForAll+"/allRawReflections.sql"), allCommandsRawReflection.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void absolutePathDatenMapping(HashMap<String, String> absolutePathDatenMap, String pathToDatenFolder){
        //BSP: KEY:  Euro2016_1.csv.bz2  VALUE:  /media/jonas/TOSHIBA EXT/Jonas/daten/PublicBIbenchmark/Euro2016/Euro2016_1.csv.bz2
        File folder = new File(pathToDatenFolder);
        File[] listOfFiles = folder.listFiles();
        //HashMap<String, String> absolutePathDatenCSV = new HashMap<String, String>();
        String ergebnis="";
        //BSP-Pfad: /media/jonas/TOSHIBA EXT/Jonas/testDaten/AirlineSentiment/AirlineSentiment_1.csv.bz2
        for (File file : listOfFiles) {
            String path2 = pathToDatenFolder + ("/" + file.getName());
            //System.out.println(path2);
            File folder2 = new File(path2);
            File[] listOfFiles2 = folder2.listFiles();
            for (File file2 : listOfFiles2) {
                if (file2.getName().endsWith("bz2")) { //wenn schon mal txt dateien erstellt wurden(aussortiert)
                    String absolutePathFile = file2.getAbsolutePath();
                    absolutePathDatenMap.put(absolutePathFile.substring(absolutePathFile.lastIndexOf("/")+1,absolutePathFile.length()), absolutePathFile);
                }
            }
        }
        //Hashmap print
        /*absolutePathDatenCSV.entrySet().forEach(entry->{
            System.out.println(entry.getKey() + " PATH_:  " + entry.getValue());
        });*/
    }
    public static String getAbsolutePathDaten(HashMap<String, String> absolutePathDatenMap, String value){
        return absolutePathDatenMap.getOrDefault(value, " ERROR1:_TYP_NOT_MAPPED ");
    }
    public static String createReflections(HashMap<String, String> typeConvertDremio, String tablePath, String speicherOrtForAll, String zielOrdnerDremio)throws FileNotFoundException{
        File text = new File(tablePath);
        Scanner scnr = new Scanner(text);
        String tableFileName = tablePath.substring(tablePath.lastIndexOf('/')+1, (tablePath.length()-10));
        String result = "ALTER DATASET "+zielOrdnerDremio+".\""+tableFileName+"\"";
        result += " CREATE AGGREGATE REFLECTION ";
        result += tableFileName+"_AGG"; //Reflection Name
        result += " USING";
        scnr.nextLine(); //überspringt erste line
        String dimensions ="";
        String measures="";
        while (scnr.hasNextLine()) { //schneidet aus jeder zeile column-name raus
            String line = scnr.nextLine();
            if (line.length() > 2) { //letzte zeile nicht lesen(ist nur klammer zu "};")
                String lineCuttet = line.substring((line.lastIndexOf('"')+2),line.length()); //macht das Typ in Liste auf Platz 0 ist
                //muss ab da abgeschnitten werden weil manche column names spaces enthalten
                List<String> myList = new ArrayList<String>(Arrays.asList(lineCuttet.split(" ")));
                String ohneKommaZumSchluss = myList.get(0).replace(",", "");//Zeile kann quasi: ("A" double,) <-

                //String decimalCuttet=ohneKommaZumSchluss.substring(0, 7);//manchmal decimal(7 oder decimal(7),
                if(ohneKommaZumSchluss.startsWith("decimal")){
                    ohneKommaZumSchluss = "decimal";
                }
                //abfangen long varbinary => 2 Wörter programm wertet nur erstes aus
                //("long varbinary","varbinary"); ("long varchar","varchar");
                if(ohneKommaZumSchluss.startsWith("long")){
                    if(myList.get(0).startsWith("varb")){
                        ohneKommaZumSchluss = "varbinary";
                        ohneKommaZumSchluss = "varchar";
                    }
                }
                if(ohneKommaZumSchluss.startsWith("varchar")){
                    ohneKommaZumSchluss = "varchar";
                }
                if(ohneKommaZumSchluss=="year"){
                    ohneKommaZumSchluss="year!";
                }
                //--muss jezz in dremio-typen und umgemappet werden
                //erst nach if abfragen => varchar(1600), decimal(2, 3), long varbinary vorbereitet zum mapping(Klammern weg, ein Wort)
                ohneKommaZumSchluss = getDremio(typeConvertDremio, ohneKommaZumSchluss);

                String columnName = line.substring(2, (line.lastIndexOf('"')+1))+","; //z.b "F1",
                //column Names "F1"-"F9", "Number of Records", "WNET (bin)",

                switch(ohneKommaZumSchluss) {
                    //year ist integer -->müsste dimension sein
                    case "float"://Aggregations
                        measures+=columnName;
                        break;
                    case "integer":
                        measures+=columnName;
                        break;
                    case"year!": //year normal als integer gemappt aber soll Dimension sein
                        dimensions+=columnName;
                        break;
                    default: //muss Measures
                        dimensions+=columnName;
                        break;
                }
                /*
                ALTER DATASET "@dremio"."Arade_1_ColumnTypesNames"
                CREATE AGGREGATE REFLECTION ABC USING
                DIMENSIONS (F1,F2)
                MEASURES (F4,F5)
                 */
            }
        }
        //letztes Komma entfernen funktioniert mit lastIndex oder substring nicht bei Column names mit leerzeichen
        //measures = measures.substring(0,measures.lastIndexOf(",")-1);
        //dimensions = dimensions.substring(0,measures.lastIndexOf(",")-1);
        result+=" DIMENSIONS (";
        if(dimensions.equals("")){ //Dimension kann nicht leer sein sonst Dremio Error
            dimensions+=measures.substring(0,measures.indexOf(",")); //holt sich den ersten Measure Wert als default wert -> einsatz falls Dimension leer
        }
        result+=dimensions;
        result=result.substring(0,(result.length()-1)); //letztes komma entfernen
        result+= ") MEASURES (";
        result+=measures;
        result=result.substring(0,(result.length()-1));
        result+=");";

        //String speicherOrt="/media/jonas/TOSHIBA EXT/Jonas/Skripte/AggReflections_einzeln";
        String speicherOrt = speicherOrtForAll+"/AggReflections_einzeln";
        File file = new File(speicherOrt);
        boolean dirCreated = file.mkdir(); //created alle Ordner des Pathes die noch nicht exe.
        speicherOrt+="/"+tableFileName+"_AGG.sql";
        try {
            Files.write(Paths.get(speicherOrt), result.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    public static String createRAWReflections(String tablePath, String speicherOrtForAll, String zielOrdnerDremio) throws FileNotFoundException {
        //RAW s über die vds created
        File text = new File(tablePath);
        Scanner scnr = new Scanner(text);
        String tableFileName = tablePath.substring(tablePath.lastIndexOf('/')+1, (tablePath.length()-10));
        String result = "ALTER DATASET "+zielOrdnerDremio+".\""+tableFileName+"\"";
        result += " CREATE RAW REFLECTION ";

        result += tableFileName+"_RAW"; //Reflection Name
        result += " USING DISPLAY (";
        scnr.nextLine(); //überspringt erste line

        while (scnr.hasNextLine()) { //schneidet aus jeder zeile column-name raus
            String line = scnr.nextLine();
            if (line.length() > 2) { //letzte zeile nicht lesen(ist nur klammer zu "};")
                String columnName = line.substring(2, (line.lastIndexOf('"')+1))+","; //z.b "F1",
                result+=columnName;
            }
        }
        result=result.substring(0,result.length()-1);
        result+=");";

        //String speicherOrt="/media/jonas/TOSHIBA EXT/Jonas/Skripte/RawReflections_einzeln";
        String speicherOrt = speicherOrtForAll+"/RawReflections_einzeln";
        File file = new File(speicherOrt);
        boolean dirCreated = file.mkdir(); //created alle Ordner des Pathes die noch nicht exe.
        speicherOrt+="/"+tableFileName+"_RAW.sql";
        try {
            Files.write(Paths.get(speicherOrt), result.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}



