/**
 * Created by jeremy on 1/04/14.
 */

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')
@Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7' )

import org.apache.log4j.*
import groovy.sql.Sql
import groovy.json.JsonSlurper

import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.POST
import groovyx.net.http.AsyncHTTPBuilder

class ApplicationPaymentProcessor {
    static counterpartyAPI
	static mastercoinAPI
    static bitcoinAPI   
    static String walletPassphrase
    static int sleepIntervalms
    static String databaseName
    static String counterpartyTransactionEncoding
    static int walletUnlockSeconds
	static satoshi = 100000000
	static appUrl
	
	private groovyx.net.http.AsyncHTTPBuilder httpAsync
	
	static assetConfig	

    static logger
    static log4j
    static db

    public init() {

        // Set up some log4j stuff
        logger = new Logger()
        PropertyConfigurator.configure("PaymentProcessor_log4j.properties")
        log4j = logger.getRootLogger()
        log4j.setLevel(Level.INFO)
		
		counterpartyAPI = new CounterpartyAPI(log4j)
		mastercoinAPI = new MastercoinAPI(log4j)
        bitcoinAPI = new BitcoinAPI()

        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("ApplicationPaymentProcessor.ini").toURL())        
        walletPassphrase = iniConfig.bitcoin.walletPassphrase
        
		sleepIntervalms = iniConfig.sleepIntervalms
        databaseName = iniConfig.database.name
        counterpartyTransactionEncoding = iniConfig.counterpartyTransactionEncoding
        walletUnlockSeconds = iniConfig.walletUnlockSeconds
		
		// Assuming only one asset
		assetConfig = Asset.readAssets("AssetInformation.ini").first()
				
		appUrl = iniConfig.applicationUrl
		// Init async http handler
        httpAsync = new AsyncHTTPBuilder(
                poolSize : 10,
                uri : appUrl,
                contentType : URLENC )

		
		// assetConfig = Asset.readAssets("AssetInformation.ini")

        // Init database
        db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
        db.execute("PRAGMA busy_timeout = 2000")
		ApplicationDBCreator.createDB(db)
    }


    public audit() {

    }

    public getNextCode() {	
		def result
        def row = db.firstRow("select * from codes where status = 'init'")

        if (row == null) {
			result = -1
        } else {       
			result = row.code
			db.execute("update codes set status='processing' where code = ${result}")
		}
        return result
    }
	
	public getTransactions(code) { 
        def result = httpAsync.request( POST,URLENC ) { req ->
            body = "code=${code}"

            response.success = { resp, hash ->
				def res = hash.iterator().next()
				def responseText = res.key				
				def json = new JsonSlurper().parseText(responseText)
				log4j.info(json.transactions)
                return json
            }

			response.failure = { resp -> 
				log4j.info(command + " failed") 
				assert resp.responseBase == null
			}

        }

        assert result instanceof java.util.concurrent.Future
        while ( ! result.done ) {
            Thread.sleep(100)
        }
			
        return result.get()	
	}
	
	public processTransaction(code, transaction) { 
		// First - commit transaction to db
		def type = Asset.COUNTERPARTY_TYPE
		log4j.info("insert into issues values (${code}, ${transaction.pk}, ${transaction.zooz}, ${type}, 'init')")
		db.execute("insert into issues values (${code}, ${transaction.pk}, ${transaction.zooz}, ${type}, 'init')")
				
		// now - try to pay transaction 
		return pay(code, transaction, type)
	}
	
	private updateStatus(code, transaction, status) { 
		log4j.info("update issues set status=${status} where code = ${code} and address = ${transaction.pk}")
		db.execute("update issues set status=${status} where code = ${code} and address = ${transaction.pk}")			
	}
		
	public pay(code, transaction, type) {
		
        def sourceAddress = ""
		def outAsset = ""	
		if (type == Asset.COUNTERPARTY_TYPE) { 
				sourceAddress = assetConfig.counterpartyAddress
				outAsset = assetConfig.counterpartyAssetName
		} else {
				sourceAddress = assetConfig.mastercoinAddress
				outAsset = assetConfig.mastercoinAssetName
		}
		def destinationAddress = transaction.pk
		def amount = transaction.zooz * satoshi
		def success = true
				       
        log4j.info("Processing payment in ${code} Sending ${outAmount} ${outAsset} from ${sourceAddress} to ${destinationAddress}")

        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)

		if (type == Asset.COUNTERPARTY_TYPE) {
			// Create the (unsigned) counterparty send transaction
			def unsignedTransaction = counterpartyAPI.createSend(sourceAddress, destinationAddress, asset, amount)
			assert unsignedTransaction instanceof java.lang.String
			assert unsignedTransaction != null
			if (!(unsignedTransaction instanceof java.lang.String)) { // catch non technical error in RPC call
				assert unsignedTransaction.code == null
			}

			// sign transaction
			def signedTransaction = counterpartyAPI.signTransaction(unsignedTransaction)
			assert signedTransaction instanceof java.lang.String
			assert signedTransaction != null

			// send transaction
			try {
				counterpartyAPI.broadcastSignedTransaction(signedTransaction)
				updateStatus(code, transaction, 'complete')
				success = true
			}
			catch (Throwable e) {
				success = false
				updateStatus(code, transaction, 'error')
			}
		} else {
			// send transaction
			try {
				mastercoinAPI.sendAsset(sourceAddress, destinationAddress, asset, amount/satoshi)
				updateStatus(code, transaction, 'complete')				
				success = true
			}
			catch (Throwable e) {
				success = false
				updateStatus(code, transaction, 'error')
			}
		}

        // Lock bitcoin wallet
        bitcoinAPI.lockBitcoinWallet()

        return success
    }


	// We don't really use the current block... 
    // This is the major thing that needs to be fixed. We shall assume that we have different addresses,
	// so that we can discover... 
	public static int main(String[] args) {
        def paymentProcessor = new ApplicationPaymentProcessor()

        paymentProcessor.init()
        paymentProcessor.audit()

        log4j.info("Application Payment processor started")        
	
        // Begin following blocks
        while (true) {
            def code = paymentProcessor.getNextCode()

            if (code != -1)  {
					def batch = paymentProcessor.getTransactions(code)					
					if (batch != null) { 
						// go over transactions, insert into db and try to pay
						// for now - don't way for approval
						def transactions = batch.transactions
						def success = true
						for (transaction in transactions) { 
							def result = paymentProcessor.processTransaction(code, transaction)
							success &= result
						}

						if (success) { 
							log4j.info("All transactions succeeded")
						} else {
							log4j.info("Some transactions failed")
						}
						
					}
			}

            sleep(sleepIntervalms)
        }

    }
}
