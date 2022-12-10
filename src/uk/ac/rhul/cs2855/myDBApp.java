package uk.ac.rhul.cs2855;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

public class myDBApp {

	public static void main(String[] argv) throws SQLException {

		Scanner sc = new Scanner(System.in);
		System.out.print("Enter your database username: ");
		String user = sc.next();
		System.out.println();
		System.out.print("Enter your database password: ");
		String password = sc.next();
		System.out.println();

		// Assuming this is running on NoMachine Linux environment without tunnelling
		String database = "teachdb.cs.rhul.ac.uk";

		Connection connection = connectToDatabase(user, password, database);
		if (connection != null) {
			System.out.println("SUCCESS: \t Database connected.\n");
		} else {
			System.out.println("ERROR: \tConnection failed. Exiting...");
			System.exit(1);
		}
		// Now we're ready to use the DB. You may add your code below this line.

	}

	// You can write your new methods here.

	// ADVANCED: This method is for advanced users only. You should not need to
	// change this!
	public static Connection connectToDatabase(String user, String password, String database) {
		System.out.println("------ Testing PostgreSQL JDBC Connection ------");
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
}
