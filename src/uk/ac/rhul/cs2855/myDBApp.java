package uk.ac.rhul.cs2855;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class myDBApp {

	/**
	 * Main method for myDBApp.
	 * 
	 * @param argv Command-line arguments
	 * @throws SQLException Thrown when error in SQL
	 */
	public static void main(String[] argv) throws SQLException {

		/**
		 * Initialising a connection to the postgres server
		 */

		// 1. The program should ask the user for his/her username and password
		Scanner sc = new Scanner(System.in);
		System.out.print("Enter your database username: ");
		String user = sc.next();
		System.out.println();
		System.out.print("Enter your database password: ");
		String password = sc.next();
		System.out.println();
		sc.close();

		// connect to the correct database based on this username and password.
		String database = "teachdb.cs.rhul.ac.uk";
		Connection connection = connectToDatabase(user, password, database);
		if (connection != null) {
			System.out.println("SUCCESS: \t Database connected.\n");
		} else {
			System.out.println("ERROR: \tConnection failed. Exiting...");
			System.exit(1);
		}

		/**
		 * Reading/writing tables in the databases
		 */

		// The program should check if the tables (and possibly views) it creates
		// already exist in the database, and only if they do, drop them before their
		// creation.
		dropTable(connection, "delayedFlights");
		dropTable(connection, "airport");

		System.out.println("Create database delayedFlights...");
		createTable(connection, """
				delayedFlights(
				    ID_of_Delayed_Flight int,
				    Month int,
				    DayofMonth int,
				    DayOfWeek int,
				    DepTime int,
				    ScheduledDepTime int,
				    ArrTime int,
				    ScheduledArrTime int,
				    UniqueCarrier char(2),
				    FlightNum char(4),
				    ActualFlightTime int,
				    scheduledFlightTime int,
				    AirTime int,
				    ArrDelay int,
				    DepDelay int,
				    Orig char(3),
				    Dest char(3),
				    Distance int,
				    primary key (ID_of_Delayed_Flight)
				);
								""");
		System.out.println(
				"Inserted " + insertIntoTableFromFile(connection, "delayedFlights", "delayedFlights") + " rows.\n");

		System.out.println("Create database delayedFlights...");
		createTable(connection, """
				airport (
				    airportCode char(3),
				    airportName char(100),
				    City char(50),
				    State char(2),
				    primary key (airportCode)
				);
								""");
		System.out.println("Inserted " + insertIntoTableFromFile(connection, "airport", "airport") + " rows.\n");

		ResultSet query1 = executeQuery(connection, """
				SELECT
				    UniqueCarrier,
				    Count(*)
				FROM
				    delayedFlights
				GROUP BY
				    UniqueCarrier
				ORDER BY
				    Count(*) DESC
				FETCH FIRST
				    5 ROWS ONLY;
								""");
		try {
			System.out.println("################## 1st Query ###############");
			while (query1.next()) {
				System.out.println(query1.getString(1) + " " + query1.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println();

		ResultSet query2 = executeQuery(connection, """
				SELECT
				    airport.City,
				    Count(*)
				FROM
				    delayedFlights
				    INNER JOIN airport ON airport.airportCode = delayedFlights.Orig
				GROUP BY
				    airport.City
				ORDER BY
				    Count(*) DESC
				FETCH FIRST
				    5 ROWS ONLY;
												""");
		try {
			System.out.println("################## 2nd Query ###############");
			while (query2.next()) {
				System.out.println(query2.getString(1).trim() + " " + query2.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println();

		ResultSet query3 = executeQuery(connection, """
				SELECT
				    Dest,
				    SUM(ArrDelay)
				FROM
				    delayedFlights
				GROUP BY
				    Dest
				ORDER BY
				    SUM(ArrDelay) DESC
				OFFSET
				    1 ROWS
				FETCH NEXT
				    5 ROWS ONLY;
																""");
		try {
			System.out.println("################## 3nd Query ###############");
			while (query3.next()) {
				System.out.println(query3.getString(1).trim() + " " + query3.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println();

		ResultSet query4 = executeQuery(connection, """
				SELECT
				    State,
				    COUNT(*)
				FROM
				    airport
				GROUP BY
				    State
				HAVING
				    Count(*) >= 10;
																				""");
		try {
			System.out.println("################## 4th Query ###############");
			while (query4.next()) {
				System.out.println(query4.getString(1).trim() + " " + query4.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println();

		ResultSet query5 = executeQuery(connection, """
				SELECT
				    DISTINCT o.State, Count(*)
				from
				    delayedFlights
				    INNER JOIN airport o on delayedFlights.Orig = o.airportCode
				    INNER JOIN airport d on delayedFlights.Dest = d.airportCode
				WHERE
				    o.State = d.State
				GROUP BY
				    o.State
				ORDER BY
				    Count(*) DESC
				FETCH FIRST
				    5 ROWS ONLY;
																								""");
		try {
			System.out.println("################## 5th Query ###############");
			while (query5.next()) {
				System.out.println(query5.getString(1).trim() + " " + query5.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static Connection connectToDatabase(String user, String password, String database) {
//		System.out.println("------ Testing PostgreSQL JDBC Connection ------");
		Connection connection = null;
		try {
			String protocol = "jdbc:postgresql://";
			String dbName = "/CS2855/";
			String fullURL = protocol + database + dbName + user;
			connection = DriverManager.getConnection(fullURL, user, password);
		} catch (SQLException e) {
			String errorMsg = e.getMessage();
			if (errorMsg.contains("authentication failed")) {
				System.out.println(
						"ERROR: \tDatabase password is incorrect. Have you changed the password string above?");
				System.out.println("\n\tMake sure you are NOT using your university password.\n"
						+ "\tYou need to use the password that was emailed to you!");
			} else {
				System.out.println("Connection failed! Check output console.");
				e.printStackTrace();
			}
		}
		return connection;
	}

	public static ResultSet executeQuery(Connection connection, String query) {
//		System.out.println("DEBUG: Executing query...");
		try {
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(query);
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void dropTable(Connection connection, String table) {
//		System.out.println("DEBUG: Dropping table if exists: " + table);
		try {
			Statement st = connection.createStatement();
			st.execute("DROP TABLE IF EXISTS " + table);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static void createTable(Connection connection, String tableDescription) {
//		System.out.println("DEBUG: Creating table...");
		try {
			Statement st = connection.createStatement();
			st.execute("CREATE TABLE " + tableDescription);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static int insertIntoTableFromFile(Connection connection, String table, String filename) {
		int numRows = 0;
		String currentLine = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			Statement st = connection.createStatement();
			// Read in each line of the file until we reach the end.
			while ((currentLine = br.readLine()) != null) {
				String[] values = currentLine.split(",");
				String composedLine = "INSERT INTO " + table + " VALUES (";
				for (int i = 0; i < values.length; i++) {
					composedLine += "'" + values[i] + "'";
					if ((values.length - i) > 1)
						composedLine += ",";
				}
				composedLine += ");";
				// Finally, execute the entire composed line.
				numRows += st.executeUpdate(composedLine);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return numRows;
	}

}
