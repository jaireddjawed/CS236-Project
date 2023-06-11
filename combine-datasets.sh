mkdir -p inputs
javac JoinDatasets.java -d classes
java -cp classes JoinDatasets customer-reservations.csv
java -cp classes JoinDatasets hotel-booking.csv
