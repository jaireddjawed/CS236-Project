import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.time.LocalDate;
import java.util.ArrayList;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

public class JoinDatasets {
  // columns are formatted as the following
  // [hotel_type,booking_status,lead_time,arrival_year,arrival_month,arrival_date_week_number,arrival_date_day_of_month,stays_in_weekend_nights,stays_in_week_nights,market_segment_type,country,avg_price_per_room,email]
  // we don't need to calculate revenue for cancelled bookings
  // start with the date of arrival and number of days stayed (by adding weekend and weekday nights)
  // then calculate the revenue by multiplying the number of days stayed by the average price per room
  // if the stay extends past the end of the month, then we separate the revenue by month and append these lines separately
  public static ArrayList<String> months = new ArrayList<String>() {{
    add("January");
    add("February");
    add("March");
    add("April");
    add("May");
    add("June");
    add("July");
    add("August");
    add("September");
    add("October");
    add("November");
    add("December");
  }};

  public static void calculateRevenueForHotelDatasetLine(String[] bookingInfo) throws IOException {
    String bookingStatus = bookingInfo[1];
    boolean isCanceled = bookingStatus.equals("1");

    if (isCanceled) return;

    int arrivalYear = Integer.parseInt(bookingInfo[3]),
        arrivalMonth = months.indexOf(bookingInfo[4]) + 1,
        arrivalDayOfMonth = Integer.parseInt(bookingInfo[6]),
        numNightsStayed = Integer.parseInt(bookingInfo[7]) + Integer.parseInt(bookingInfo[8]);

    double avgPricePerRoom = Double.parseDouble(bookingInfo[11]),
           revenueInCurrentMonth = 0;

    // if the average price per room is 0, then there is no need to calculate revenue
    if (avgPricePerRoom == 0) return;

    LocalDate arrivalDate = LocalDate.of(arrivalYear, arrivalMonth, arrivalDayOfMonth),
              departureDate = arrivalDate.plusDays(numNightsStayed); 

    while (arrivalDate.isBefore(departureDate)) {
      // if adding a day to the arrival date causes the month to change, reset the revenueInCurrentMonth variable
      // and add the revenue line to the combined csv file
      if (arrivalDate.getMonth() != arrivalDate.plusDays(1).getMonth()) {
        FileWriter fr = new FileWriter("./inputs/combined.csv", true);
        fr.write(
          arrivalDate.getYear() + "," +
          arrivalDate.getMonth() + "," +
          revenueInCurrentMonth + "\n"
        );
        fr.close();
        revenueInCurrentMonth = 0;
      }

      revenueInCurrentMonth += avgPricePerRoom;
      arrivalDate = arrivalDate.plusDays(1);
    }

    // add the last revenue line to the combined csv file
    FileWriter fr = new FileWriter("./inputs/combined.csv", true);
    fr.write(
      arrivalDate.getYear() + "," +
      arrivalDate.getMonth() + "," +
      revenueInCurrentMonth + "\n"
    );
    fr.close();
  }

  // columns are formatted as the following
  // [Booking_ID,stays_in_weekend_nights,stays_in_week_nights,lead_time,arrival_year,arrival_month,arrival_date,market_segment_type,avg_price_per_room,booking_status]
  public static void calculateRevenueForCustomerReservationLine(String[] bookingInfo) throws IOException {
    String bookingStatus = bookingInfo[9];
    boolean isCanceled = bookingStatus.equals("Canceled");

    if (isCanceled) return;

    int arrivalYear = Integer.parseInt(bookingInfo[4]),
        arrivalMonth = Integer.parseInt(bookingInfo[5]),
        arrivalDayOfMonth = Integer.parseInt(bookingInfo[6]),
        numNightsStayed = Integer.parseInt(bookingInfo[1]) + Integer.parseInt(bookingInfo[2]);

    double avgPricePerRoom = Double.parseDouble(bookingInfo[8]),
           revenueInCurrentMonth = 0;

    // dataset is known to have an invalid date (2018-02-29), so we subtract 1 from the day to get the correct date
    if (arrivalYear == 2018 && arrivalMonth == 2 && arrivalDayOfMonth == 29) arrivalDayOfMonth--;
    // if the average price per room is 0, then there is no need to calculate revenue
    if (avgPricePerRoom == 0) return;

    LocalDate arrivalDate = LocalDate.of(arrivalYear, arrivalMonth, arrivalDayOfMonth),
              departureDate = arrivalDate.plusDays(numNightsStayed);

    while (arrivalDate.isBefore(departureDate)) {
      // if adding a day to the arrival date causes the month to change, reset the revenueInCurrentMonth variable
      // and add the revenue line to the combined csv file
      if (arrivalDate.getMonth() != arrivalDate.plusDays(1).getMonth()) {
        FileWriter fr = new FileWriter("./inputs/combined.csv", true);
        fr.write(
          arrivalDate.getYear() + "," +
          arrivalDate.getMonth() + "," +
          revenueInCurrentMonth + "\n"
        );
        fr.close();
        revenueInCurrentMonth = 0;
      }

      revenueInCurrentMonth += avgPricePerRoom;
      arrivalDate = arrivalDate.plusDays(1);
    }

    // add the last revenue line to the combined csv file
    FileWriter fr = new FileWriter("./inputs/combined.csv", true);
    fr.write(
      arrivalDate.getYear() + "," +
      arrivalDate.getMonth() + "," +
      revenueInCurrentMonth + "\n"
    );
    fr.close();
  }

  public static void main(String[] args) {
    String reservationFile = args[0];

    try {
      // add the header line to the combined csv file if the file doesn't exist
      boolean isFileEmpty = new File("./inputs/combined.csv").length() == 0;
      if (isFileEmpty) {
        FileWriter fr = new FileWriter("./inputs/combined.csv", true);
        fr.write("Year,Month,Revenue\n");
        fr.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(reservationFile));
      String line;

      // skip the header line
      String headers = br.readLine();

      while ((line = br.readLine()) != null) {
        String[] bookingInfo = line.split(",");

        if (reservationFile.equals("hotel-booking.csv")) {
          calculateRevenueForHotelDatasetLine(bookingInfo);
        } else if (reservationFile.equals("customer-reservations.csv")) {
          calculateRevenueForCustomerReservationLine(bookingInfo);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
