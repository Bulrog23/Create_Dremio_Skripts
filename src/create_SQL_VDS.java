import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

//created&dropt nur PDS zu VDS OHNE types&columnnames setting

public class create_SQL_VDS {
    public static void main(String args[]){
        //IN dremio müssen alle Dateien PDS sein
        //notitz an mich: nur pfadinpc ändern (wenn nicht test)&NAS

        //test Einzeldatei: String pfadInPC = "/media/jonas/TOSHIBA EXT/Jonas/testDaten/AirlineSentiment/AirlineSentiment_1.csv.bz2";
        String pfadInPC = "/media/jonas/TOSHIBA EXT/Jonas/daten/PublicBIbenchmark";
        //testDaten: String pfadInPC = "/media/jonas/TOSHIBA EXT/Jonas/testDaten";
        String pfadNASDremio ="allData."; //+folderName +dateiName (Arade_1.csv.bz2")-->aus Datei ausgelesen

        //für Dirk Server
        String speicherFolderDropAll = "/media/jonas/TOSHIBA EXT/Jonas/SkripteDirkServer";
        String speicherFolderCreatVDSOhneNameTypes="/media/jonas/TOSHIBA EXT/Jonas/SkripteDirkServer/all_VDS_OhneTypNames.txt";

        //für local Server
        //String speicherFolderDropAll="/media/jonas/TOSHIBA EXT/Jonas/Skripte/sqlQueries_bearbeitet_einzeln";
        //String speicherFolderCreatVDSOhneNameTypes="/media/jonas/TOSHIBA EXT/Jonas/Skripte/sqlQueries_bearbeitet_einzeln/all_VDS_OhneTypNames.txt";

        //String zielOrdnerDremio = "\"@dremio\""; //localHostServer, Hauptordner=Username ->DateiName auto erstellt
        String zielOrdnerDremio = "\"@greim\""; //dirk Server

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
            Files.write(Paths.get(speicherPath_drop), allDrop.getBytes()); //erstellt txt datei mit Namen wird durch letzten \ ende des Pathes gesetzt
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
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

    public static String sqlVDSLoop(String pfadInPC, String pfadNASDremio, String zielDremio){
        File folder = new File(pfadInPC);
        File[] listOfFiles = folder.listFiles();
        String ergebnis="";
        //BSP-Pfad: /media/jonas/TOSHIBA EXT/Jonas/testDaten/AirlineSentiment/AirlineSentiment_1.csv.bz2
        for (File file : listOfFiles) {
            String path2 = pfadInPC + ("/" + file.getName());
            //System.out.println(path2);
            File folder2 = new File(path2);
            File[] listOfFiles2 = folder2.listFiles();
            for (File file2 : listOfFiles2) {
                if(file2.getName().endsWith("bz2")) { //wenn schon mal txt dateien erstellt wurden(aussortiert)
                    String absolutePathFile=file2.getAbsolutePath();
                    ergebnis+=createSQL(absolutePathFile,pfadNASDremio,zielDremio);
                    //ergebnis+=absolutePathFile+"\n";
                }
            }
        }
        return ergebnis;
    }
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
