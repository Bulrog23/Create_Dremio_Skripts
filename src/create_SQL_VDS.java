import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

//NUR BEISPIEL KLASSE (Idee mit Name&Typ settings für CSV-Dateien in Aufbereitung_Datasets_Reflections vervollständigt)
//wird nicht genutzt

//Erstellt Anfragen um Virtual Datasets(VDS) für Physical Datasets(PDS)(hier: =CSV-Daten) zu erstellen
//---> OHNE TYP& NAME SETTINGS (--> wird im Main gemacht)
//Erstellt auch Lösch-Anfragen für die erstellten VDS. (sonst in UI manuell lösche oder Server reset)

public class create_SQL_VDS {

    //Diese Klasse erstellt die SQL-Anfragen mit denen VDS der CSV-Dateien in Dremio erstellt werden
    //---> OHNE TYP& NAME SETTINGS (--> wird im Main gemacht)
    //Es wird mit der SQL-Anfrage ein VDS(Virtual Dataset) erstellt indem alles richtig gesetzt wird
    //dadurch müsssen CSV Dateien nicht aufwendig in der UI manuell aufbereitet werden (manuell Typen und Namen setzten --> sehr Zeit intensiv)
    //Die CSV-Dateien können auch nicht einfach unaufbereitet die Anfragen des Public bi Benchmarks ausführen
    //Obwohl die Public bi Benchmark Anfragen Casts und As ausführen -> viele Datentypen erkennt Dremio nicht, setzt sie falsch oder erkennt Nulls nicht
    //wenn die Typen&Namen richtig gesetzt sind kann man die public bi Benchmark Anfragen ausführen

    public static void main(String args[]){
        //In Dremio sind alle geladenen Datasets PDS (physical Datasets)

        String pfadInPC = "/media/jonas/TOSHIBA EXT/Jonas/daten/PublicBIbenchmark"; //Pfad zu den PublicBIBenchmark
        String pfadNASDremio ="allData."; //Name des NAS Folders in Dremio

        //Speicherort für die Aufbereitungen
        String speicherFolderCreatVDSOhneNameTypes="/media/jonas/TOSHIBA EXT/Jonas/Skripte/sqlQueries_bearbeitet_einzeln/all_VDS_OhneTypNames.txt";

        String speicherFolderDropAll="/media/jonas/TOSHIBA EXT/Jonas/Skripte/sqlQueries_bearbeitet_einzeln"; //Speicherort angeben der bearbeiteten SQL Anfragen
        //mit ihnen werden die DROP Befehle erstellt (Löschbefehle)

        String zielOrdnerDremio = "\"@dremio\""; //localHostServer: Hauptordner==Username (->default DateiName)

        //CREATE
        String allConverts = sqlVDSLoop(pfadInPC, pfadNASDremio, zielOrdnerDremio);
        //System.out.println(allConverts);
        try {
            Files.write(Paths.get(speicherFolderCreatVDSOhneNameTypes), allConverts.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }

        //DROP
        String allDrop = dropVDSLoop(allConverts);
        //System.out.println(allDrop);
        String speicherPath_drop = speicherFolderDropAll + "/allDropVDS.txt"; //datei-Name
        try {
            Files.write(Paths.get(speicherPath_drop), allDrop.getBytes()); //erstellt txt datei mit Namen
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // diese Methode erstellt eine SQL-Aufbereitungs-Anfrage für den gewünschten Dataset
    public static String createSQL(String absolutePfadInPC, String pfadNASDremio, String zielDremio){
        //BSP: CREATE VDS "@dremio".newName as SELECT * FROM test.Arade."Arade_1.csv.bz2"

        String sqlCreationVDS="CREATE VDS ";
        //here dremioPfad: "@dremio".newName == newname dateiName ohne .csv.bz2
        sqlCreationVDS += zielDremio+".";
        sqlCreationVDS += absolutePfadInPC.substring(absolutePfadInPC.lastIndexOf("/")+1,absolutePfadInPC.length()-8);
        sqlCreationVDS +=" as SELECT * FROM ";
        //here: NAS.Folder."CSV-NAME"
        sqlCreationVDS += pfadNASDremio;
        String dateiName = absolutePfadInPC.substring(absolutePfadInPC.lastIndexOf("/")+1);
        String letzterQuer= absolutePfadInPC.substring(0, absolutePfadInPC.lastIndexOf("/"));
        String folderName= letzterQuer.substring(letzterQuer.lastIndexOf("/")+1);
        sqlCreationVDS+= folderName+".\""+dateiName+"\"";
        sqlCreationVDS+=";\n";
        //System.out.println(sqlCreationVDS);
        return sqlCreationVDS;
    }

    // diese Methode loopt durch den public Bi Benchmark und erstellt für jeden Dataset die Aufbereitungen
    public static String sqlVDSLoop(String pfadInPC, String pfadNASDremio, String zielDremio){
        File folder = new File(pfadInPC);
        File[] listOfFiles = folder.listFiles();
        String ergebnis="";
        //Beispiel-Pfad: /media/jonas/TOSHIBA EXT/Jonas/testDaten/AirlineSentiment/AirlineSentiment_1.csv.bz2
        for (File file : listOfFiles) {
            String path2 = pfadInPC + ("/" + file.getName());;
            File folder2 = new File(path2);
            File[] listOfFiles2 = folder2.listFiles();
            for (File file2 : listOfFiles2) {
                if(file2.getName().endsWith("bz2")) { //wenn schon mal txt dateien erstellt wurde -> aussortiert
                    String absolutePathFile=file2.getAbsolutePath();
                    ergebnis+=createSQL(absolutePathFile,pfadNASDremio,zielDremio);
                    //ergebnis+=absolutePathFile+"\n";
                }
            }
        }
        return ergebnis;
    }

    //hier werden auch gleichzeitig Lösch Befehle für die erstellten VDS erstellt
    //in UI müssten alle Manuell gelöscht werden oder Server reset (bei 200 -> sehr aufwendig)
    public static String dropVDS(String createVDS){
        //String createVDS = "CREATE VDS \"@dremio\".Medicare1_2 as SELECT * FROM test.Medicare1.\"Medicare1_2.csv.bz2\";";
        createVDS = createVDS.substring(6);
        createVDS = "DROP"+createVDS;
        createVDS= createVDS.substring(0,createVDS.lastIndexOf("SELECT")-4);
        createVDS += ";";
        //System.out.println(createVDS);
        return createVDS;
    }
    public static String dropVDSLoop(String sqlCreateQuery){
        String[] lines = sqlCreateQuery.split(System.getProperty("line.separator"));
        String dropErgebnis="";
        for(String einzeln: lines) {
            dropErgebnis+=(dropVDS(einzeln)+"\n");
        }
        return dropErgebnis;
    }


}
