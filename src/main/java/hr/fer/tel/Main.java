package hr.fer.tel;

import hr.fer.tel.apsimx.APSIMX_File;
import hr.fer.tel.location.Locations;

import java.net.*;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class Main {
    private static final int N_YEARS = -5;

    private static Map<String, Double> Radiation = new HashMap<>();
    private static Map<String, Double> MaxT = new HashMap<>();
    private static Map<String, Double> MinT = new HashMap<>();
    private static Map<String, Double> Rain = new HashMap<>();
    private static Map<String, Double> Wind = new HashMap<>();

    public static void main(String[] args) { //add input parameters for location (selected lan and lon from this input parameter)
        //DownloadWeatherData();
        //PrepareDownloadedData();
        ReadDownloadedData();
        InsertDataIntoFile();
    }

    private static void InsertDataIntoFile(){
        try {
            FileWriter fw = new FileWriter("C:\\text.met", true);
            BufferedWriter bw = new BufferedWriter(fw);

            Calendar calOne = Calendar.getInstance();
            int dayOfYear = calOne.get(Calendar.DAY_OF_YEAR);
            int year = calOne.get(Calendar.YEAR);
            DateFormat dateFormat = new SimpleDateFormat("MM-dd");

            for (int i = dayOfYear; i <= 365; i++){
                calOne.set(Calendar.DAY_OF_YEAR, i);
                String dateKey = dateFormat.format(calOne.getTime());
                String stringToInsert = String.format("%d %d %.1f %.1f %.1f %.1f %.1f 999999",
                        year, i, Radiation.get(dateKey), MaxT.get(dateKey), MinT.get(dateKey), Rain.get(dateKey), Wind.get(dateKey));
                System.out.println(stringToInsert);
                bw.write(stringToInsert);
                bw.newLine();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void DownloadWeatherData(){
        URL website = null;
        String yesterday = getYesterdayDateString();

        try {
            String url = String.format("https://worldmodel.csiro.au/gclimate?lat=%s&lon=%s&format=apsim&start=%s&stop=%s",
                    Locations.ZAGREB_LAT, Locations.ZAGREB_LON, getYear(N_YEARS), yesterday); //date format: yyyyMMdd
            website = new URL(url);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream("C:\\text.met"); //change text file depending on location (npr. Zagreb.met if Zagreb is selected).
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void PrepareDownloadedData(){
        CreateApsimxFile();
        WriteToApsimxFile();
        RunSimulation();
    }

    private static void ReadDownloadedData(){
        getData();
    }

    private static Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:C://weather.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    private static void getData(){
        String sql = "SELECT * FROM Report";

        try {
            Connection conn = connect();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            int remainingDaysUntilEndOfYear = GetNumberOfDaysUntilEndOfYear();

            // loop through the result set
            while (rs.next()) {
                Date databaseDate = rs.getDate("Clock.Today");
                DateFormat dateFormat = new SimpleDateFormat("MM-dd");
                Calendar cal = Calendar.getInstance();
                for (int i = 0; i < remainingDaysUntilEndOfYear+1; i++){
                    if (i == 0){
                        cal.add(Calendar.DATE, 0);
                    }
                    else{
                        cal.add(Calendar.DATE, 1);
                    }
                    if (dateFormat.format(cal.getTime()).equals(dateFormat.format(databaseDate))){
                        if (rs.getDouble("Weather.Radn") != -999){
                            InsertDataIntoMap(Radiation, dateFormat.format(cal.getTime()), rs.getDouble("Weather.Radn"));
                        }
                        if (rs.getDouble("Weather.MaxT") != -999){
                            InsertDataIntoMap(MaxT, dateFormat.format(cal.getTime()), rs.getDouble("Weather.MaxT"));
                        }
                        if (rs.getDouble("Weather.MinT") != -999){
                            InsertDataIntoMap(MinT, dateFormat.format(cal.getTime()), rs.getDouble("Weather.MinT"));
                        }
                        if (rs.getDouble("Weather.Rain") != -999){
                            InsertDataIntoMap(Rain, dateFormat.format(cal.getTime()), rs.getDouble("Weather.Rain"));
                        }
                        if (rs.getDouble("Weather.Wind") != -999){
                            InsertDataIntoMap(Wind, dateFormat.format(cal.getTime()), rs.getDouble("Weather.Wind"));
                        }
                    }
                }
            }
            conn.close();

            CalculateAverageWeatherValue(Radiation, N_YEARS*(-1));
            CalculateAverageWeatherValue(MaxT, N_YEARS*(-1));
            CalculateAverageWeatherValue(MinT, N_YEARS*(-1));
            CalculateAverageWeatherValue(Rain, N_YEARS*(-1));
            CalculateAverageWeatherValue(Wind, N_YEARS*(-1));

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void CalculateAverageWeatherValue(Map<String, Double> data, int n){
        for (String date : data.keySet()){
            Double currentValue = data.get(date);
            currentValue = currentValue / (double)n;
            data.put(date, currentValue);
        }
    }

    private static void InsertDataIntoMap(Map<String, Double> data, String date, Double dataToInsert){
        if (data.containsKey(date)){
            Double currentValue = data.get(date);
            currentValue += dataToInsert;
            data.put(date, currentValue);
        }
        else{
            data.put(date, dataToInsert);
        }
    }

    private static int GetNumberOfDaysUntilEndOfYear(){
        Calendar calOne = Calendar.getInstance();
        int dayOfYear = calOne.get(Calendar.DAY_OF_YEAR);
        int year = calOne.get(Calendar.YEAR);
        Calendar calTwo = new GregorianCalendar(year, 11, 31);
        int day = calTwo.get(Calendar.DAY_OF_YEAR);
        int total_days = day - dayOfYear;
        System.out.println("Total " + total_days + " days remaining in "+year);
        return total_days;
    }

    private static void WriteToApsimxFile(){ //input paramet za koji file (maize, potato,...)
        try {
            FileWriter myWriter = new FileWriter("C:\\Weather.apsimx");
            myWriter.write(APSIMX_File.WEATHER_FILE);
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static void CreateApsimxFile(){ //input paramet za koji file (maize, potato,...)
        try {
            File myObj = new File("C:\\Maize.apsimx");
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private static void RunSimulation(){
        ProcessBuilder processBuilder = new ProcessBuilder();
        // Windows
        processBuilder.command("cmd.exe", "/c", "dotnet apsim.dll run f Maize.apsimx");

        try {

            Process process = processBuilder.start();

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = process.waitFor();
            System.out.println("\nExited with error code : " + exitCode);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String getYear(int index) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar year = Calendar.getInstance();
        year.add(Calendar.YEAR, index);
        return dateFormat.format(year.getTime());
    }

    private static Date yesterday() {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return cal.getTime();
    }

    private static String getYesterdayDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return dateFormat.format(yesterday());
    }
}
