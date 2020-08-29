/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.*;
/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice



		//verify lengths
	public static void AddCustomer(MechanicShop esql){//1
        String insertCustomer = "";
        int id = 5961;
		String fname =  "joe";
        String lname = "mama";
        String phone = "(123)456-7890";
        String address = "123 wow street";
        
        //assuming that the person adding in the input knows the next ID
        System.out.println("Insert an unused Customer ID: ");
		try {
		id = Integer.parseInt(in.readLine());
        } 
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}



        System.out.println("Insert first name: ");
		try {
		fname = in.readLine();
        	
		if(fname.length() <= 0 || fname.length() > 32){
			throw new Exception("First name is either too long (over 32), or not long enough (0)... figure it out.");
		}
		}
		//break;  
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
        
        System.out.println("Insert last name: ");
        try {
		lname = in.readLine();
        
        if(lname.length() <= 0 || lname.length() > 32){
			throw new Exception("Last name is either too long (over 32), or not long enough (0)... figure it out.");
		}
		}
		//break;
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
        //try catch good up to here
		System.out.println("Insert phone number: ");
        try {
		phone = in.readLine();

        if(phone.length() <= 0 || phone.length() > 13){
			throw new Exception("Number should be in this format: (XXX)XXX-XXXX");
		}
		}
		//break;
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
        //yes
		System.out.println("Insert address: ");
        try {
		address = in.readLine();

        if(address.length() <= 0 || address.length() > 256){
			throw new Exception("Address is either too long (over 256), or not long enough (0)... figure it out.");
		}
		}
		//break;
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}                
		
		insertCustomer = String.format( "INSERT INTO Customer (id, fname, lname, phone, address) Values ( %d,'%s', '%s', '%s', '%s');",id, fname, lname, phone, address);
        System.out.println(insertCustomer);
		
		try {
		esql.executeUpdate(insertCustomer);
		
		}
		catch (Exception e) {
		System.out.println("Something wrong with query");
		
		} 	
	}
//verify lengths
public static void AddMechanic(MechanicShop esql){//2
        int id = 5961;
        String fname =  "joe";
        String lname = "mama";
	int years = 0;
		
		//assuming that the person adding in the input knows the next ID
        System.out.println("Insert an unused Customer ID: ");
		try {
		id = Integer.parseInt(in.readLine());
        } 
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}



        System.out.println("Insert first name: ");
		try {
		fname = in.readLine();

        if(fname.length() <= 0 || fname.length() > 32){
			throw new Exception("First name is either too long (over 32), or not long enough (0)... figure it out.");
		}
		}
		//break;  
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
        
        System.out.println("Insert last name: ");
        try {
		lname = in.readLine();
        if(lname.length() <= 0 || lname.length() > 32){
			throw new Exception("Last name is either too long (over 32), or not long enough (0)... figure it out.");
		}
		}
		//break;
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}

		System.out.println("Insert years of experience: ");
		try {
		years = Integer.parseInt(in.readLine());
        

        if(years < 0 || lname.length() > 99){
			throw new Exception("There are either too many years of experience (over 99), or not enough (0)... figure it out.");
		}
		}
		//break;
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}

		String insertMechanic =	String.format("INSERT INTO Mechanic Values (%d, '%s', '%s', %d);", id, fname, lname, years);
		System.out.println(insertMechanic);

        try {
        esql.executeUpdate(insertMechanic);

        }
        catch (Exception e) {
        System.out.println("Nope");

        }
  
	}
	






	
public static void AddCar(MechanicShop esql){//3
		String vin = "v34575s";
		String make = "vehicle";
		String model = "brand";
		int year = 2000;
		
		System.out.println("Insert VIN: ");
		try {
		vin = in.readLine();
        }
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}

		System.out.println("Insert make: ");
		try {
		make = in.readLine();
        }
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}

		System.out.println("Insert model: ");
		try {
		model = in.readLine();
        }
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}

		System.out.println("Insert year: ");
		try {
		year = Integer.parseInt(in.readLine());
        }
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}


		String insertCar = String.format("INSERT INTO Car Values ('%s', '%s', '%s', %d);", vin, make, model, year);
	try {
        esql.executeUpdate(insertCar);

        }
        catch (Exception e) {
        System.out.println("Nope");

        }

	}
	
	

















	public static void InsertServiceRequest(MechanicShop esql){//4
		
	}
	
	


	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		int wid, rid, mid;
		String date, comment;
		int bill;
		
		
		//assuming person inputing knows the next id
		System.out.println("Insert an unused WID: ");
		try {
		wid = Integer.parseInt(in.readLine());
        } 
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
		
		//assuming person inputing knows the next id
		System.out.println("Insert an unused RID: ");
		try {
		rid = Integer.parseInt(in.readLine());
        } 
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
		
		//assuming person inputing knows the next id
		System.out.println("Insert an unused MID: ");
		try {
		mid = Integer.parseInt(in.readLine());
        } 
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
		
		System.out.println("Insert closing date: ");
		try {
		date = in.readLine();
        

        if(date.length() <= 0 || date.length() > 10){
			throw new Exception("Date format is YEAR-MM-DD");
		}	
		}
		//break;  
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
		
		//assuming comments will be short 
		System.out.println("Insert comment: ");
		try {
		comment = in.readLine();
        

        if(comment.length() <= 0){
			throw new Exception("Comment is blank");
		}
		}
		//break;  
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
		
		//assuming person knows the next bill
		System.out.println("Insert an unissued bill: ");
		try {
		bill = Integer.parseInt(in.readLine());
        } 
		catch (Exception e) {
		System.out.println("Invalid input");
		
		}
		
		
		
		String query = "SELECT COUNT(1) FROM Mechanic WHERE id = mid;";
		int mechanicexists = esql.executeQuery(query);
		if (mechanicexists == 0 ){
			throw new Exception("Mechanic does not exist.");
		}	
		//break;
			
		String q2 = "SELECT COUNT(1) FROM Service_Request WHERE id = rid;";
		int ridexists = esql.executeQuery(q2);
		if (ridexists == 0){
			throw new Exception("RID does not exist.");
		}	
		//break;
		
	}
	
	
	
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
        String query = "Select C.fname, C.lname, CR.date, CR.Comment, CR.bill From Customer C, Service_request SR, Closed_request CR Where C.id = SR.customer_id and SR.rid = CR.rid and CR.bill < 100;";
	try {
	esql.executeQueryAndPrintResult(query);
        }
        catch (Exception e) {
        System.out.println("Nope");	
	}
        }
	




public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7

	String query = "SELECT fname, lname FROM Customer C WHERE C.id IN (SELECT customer_id FROM Owns GROUP BY customer_id HAVING COUNT(*) > 20);"; 
	try{ 
	esql.executeQueryAndPrintResult(query);
	}
	catch(Exception e) {
		 System.out.println("Nope");
	}	
}	


	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
	String query = "SELECT C1.make, C1.model, C1.year FROM Car C1 WHERE C1.vin IN ( SELECT C.vin FROM Car C, Service_Request S  WHERE C.vin = S.car_vin AND S.odometer < 50000  AND C.year < 1995);";
	String q2 = "Select C.make, C.model, C.year From Car C, Service_Request S where C.vin = S.car_vin and S.odometer < 50,000 and C.model < 1995;"; 
	try {
        esql.executeQueryAndPrintResult(query);
		esql.executeQueryAndPrintResult(q2);
        }
	catch(Exception e){
                System.out.println("Nope");
        }
	
	}
	















	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
	int k = 0;
	System.out.println("Input k");
	try{
	k = Integer.parseInt(in.readLine());
	
	
	if(k <= 0){
			throw new Exception("Choose a better value (at least 1).");
		}
	}
	//break;
	catch (Exception e)
	{
	System.out.println("Nope");
	}
	
	String query = "Select C.make, C.model, Count(C.vin) as NumberOfRequests From Car C, Service_Request SR Where C.vin = SR.car_vin Group By C.vin  Order By NumberOfRequests Desc Limit " + k + ";";

	try{
	esql.executeQueryAndPrintResult(query);
	}
	catch (Exception e)
	{
	System.out.println("Nope");
	}
		
	}
	
	







	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		
        String query = "Select C.fname, C.lname, SUM(CR.bill) as TotalBill From Customer C, Service_Request SR, Closed_Request CR Where C.id = SR.customer_id and SR.rid = CR.rid Group By C.id Order By TotalBill Desc;";

	try {
	esql.executeQueryAndPrintResult(query);
        }
	catch (Exception e)
	{
	System.out.println("Nope");
	}
	}


	
}
