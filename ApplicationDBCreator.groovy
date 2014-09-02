/**
 * Created by amirza on 19/07/14.
 */

public class ApplicationDBCreator {
	public static createDB(db) { 
	
		db.execute("create table if not exists codes (code integer, status string, confirmcode integer)")
		db.execute("create table if not exists issues (code integer, address string, amount integer, type string, status string, txid string)")		
		
        db.execute("create unique index if not exists codes1 on codes(code)")
        db.execute("create index if not exists issues1 on issues(code, address)")
		
		def row
		// Check vital tables exist
        row = db.firstRow("select name from sqlite_master where type='table' and name='codes'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='issues'")
        assert row != null        
	}
}
