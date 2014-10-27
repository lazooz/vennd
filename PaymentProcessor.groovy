/**
 * Created by jeremy on 1/04/14.
 */

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')

import org.apache.log4j.*
import groovy.sql.Sql
import java.util.Timer

class PaymentProcessor {
	static satoshi = 100000000

    static counterpartyAPI
	static mastercoinAPI
    static bitcoinAPI    
	static bitcoinRateAPI
    static String walletPassphrase
    static int sleepIntervalms
    static String databaseName
    static String counterpartyTransactionEncoding
    static int walletUnlockSeconds
	
	static assetConfig	

    static logger
    static log4j
    static db
	
	static int currentBTCValueInUSD
	static int exchangeRateUpdateRate


    class Payment {
        def blockIdSource
        def txid
        def sourceAddress
        def destinationAddress
        def outAsset
		def outAssetType        
        def status
        def lastUpdatedBlockId
		def inAssetType
		def inAmount

        public Payment(blockIsSourceValue, txidValue, sourceAddressValue, inAssetTypeValue, inAmountValue, destinationAddressValue, outAssetValue, outAssetTypeValue, statusValue, lastUpdatedBlockIdValue) {
            blockIdSource = blockIsSourceValue
            txid = txidValue
            sourceAddress = sourceAddressValue
            destinationAddress = destinationAddressValue
            outAsset = outAssetValue
			outAssetType = outAssetTypeValue
			inAssetType = inAssetTypeValue
			inAmount = inAmountValue            
            status = statusValue
            lastUpdatedBlockId = lastUpdatedBlockIdValue
        }
    }


    public init() {

        // Set up some log4j stuff
        logger = new Logger()
        PropertyConfigurator.configure("PaymentProcessor_log4j.properties")
        log4j = logger.getRootLogger()
        log4j.setLevel(Level.INFO)
		
		counterpartyAPI = new CounterpartyAPI(log4j)
		mastercoinAPI = new MastercoinAPI(log4j)
        bitcoinAPI = new BitcoinAPI()
		
		bitcoinRateAPI = new BitcoinRateAPI()

        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("PaymentProcessor.ini").toURL())        
        walletPassphrase = iniConfig.bitcoin.walletPassphrase
        sleepIntervalms = iniConfig.sleepIntervalms
        databaseName = iniConfig.database.name
        counterpartyTransactionEncoding = iniConfig.counterpartyTransactionEncoding
        walletUnlockSeconds = iniConfig.walletUnlockSeconds
		
		assetConfig = Asset.readAssets("AssetInformation.ini")

        // Init database
        db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
        db.execute("PRAGMA busy_timeout = 2000")
		DBCreator.createDB(db)
		
		currentBTCValueInUSD = bitcoinRateAPI.getAveragedRate()
		
		log4j.info("Exchange rate is: ${currentBTCValueInUSD} USD for 1 BTC")
		
		// every 20 minutes
		def exchangeRateUpdateRate = 20 * 60 *  1000
		
		Timer timer = new Timer()
		timer.scheduleAtFixedRate(new BTCUSDRateUpdateTask(), exchangeRateUpdateRate, exchangeRateUpdateRate)
    }


    public audit() {

    }
	
	static class BTCUSDRateUpdateTask extends TimerTask { 
		public void run() {
			PaymentProcessor.currentBTCValueInUSD = PaymentProcessor.bitcoinRateAPI.getAveragedRate()
			PaymentProcessor.log4j.info("Updated exchange rate is: ${PaymentProcessor.currentBTCValueInUSD} USD for 1 BTC")
		}
	}

    public getLastPaymentBlock {
        def row

        row = db.firstRow("select max(lastUpdatedBlockId) from payments where status in ('complete')")

        if (row == null || row[0] == null) {
            return 0
        } else {
			return row[0]
        }
    }


    public getNextPayment() {
        def Payment result 
        def row

        row = db.firstRow("select * from payments where status='authorized' order by blockId")

        if (row == null || row[0] == null) {
			result = null
        } else {           
            else {
                def blockIdSource = row.blockId
                def txid = row.SourceTxid
                def sourceAddress = row.sourceAddress
                def destinationAddress = row.destinationAddress
                def outAsset = row.outAsset               
                def status = row.status
                def lastUpdated = row.lastUpdatedBlockId
				def outAssetType = row.outAssetType
				def inAssetType = row.inAssetType
				def inAmount = row.inAmount
				
                result = new Payment(blockIdSource, txid, sourceAddress, inAssetType, inAmount, destinationAddress, outAsset, outAssetType, status, lastUpdated)
            }
        }

        return result
    }
	
	def get_total_counterparty(String asset) { 
        def getAssetInfo = counterpartyAPI.getAssetInfo(asset)
		return getAssetInfo.supply[0]
	}
	
	def get_total_mastercoin(String asset) {
		def getAssetInfo = mastercoinAPI.getAssetInfo(asset)		
		return getAssetInfo.totaltokens
	}
	
	def get_counterparty_asset_balance(String sourceAddress, String asset) {
		def balances = counterpartyAPI.getBalances(sourceAddress)
		def asset_balance = 0


        for (balance in balances) {
            if (balance.asset == asset) 
                asset_balance = balance.quantity
        }
		
		return asset_balance
	}
	
	def get_mastercoin_asset_balance(String sourceAddress, String asset) {
		def balance = mastercoinAPI.getAssetBalance(sourceAddress,asset)
		return balance
	}
	
	private findAssetConfig(Payment payment) {
		for (assetRec in assetConfig) {
			if (payment.sourceAddress == assetRec.nativeAddressCounterparty || payment.sourceAddress == assetRec.nativeAddressMastercoin) { 
				return assetRec
			} 
		}
		return null
	}

	// Returns total number in satoshi	
	private get_counterparty_tokens_inuse(Asset relevantAsset) {
        def counterparty_sourceAddress = relevantAsset.nativeAddressCounterparty
		def counterparty_numberOfTokenIssued = get_total_counterparty(relevantAsset.counterpartyAssetName) 
		def counterparty_asset_balance = get_counterparty_asset_balance(counterparty_sourceAddress,relevantAsset.counterpartyAssetName)
		return counterparty_numberOfTokenIssued-counterparty_asset_balance
	}
	
	// Returns total number in satoshi
	private get_mastercoin_tokens_inuse(Asset relevantAsset) {
		def mastercoin_sourceAddress  = relevantAsset.nativeAddressMastercoin
		def mastercoin_numberOfTokenIssued = get_total_mastercoin(relevantAsset.mastercoinAssetName) * satoshi 
		def mastercoin_asset_balance = get_mastercoin_asset_balance(mastercoin_sourceAddress,relevantAsset.mastercoinAssetName) * satoshi
		return mastercoin_numberOfTokenIssued-mastercoin_asset_balance
	}

	private getTotalTokensInUse(Asset asset) {
		return get_mastercoin_tokens_inuse(asset) + get_counterparty_tokens_inuse(asset)
	}
	
    public pay_dividend(Long currentBlock, Payment payment, dividend_amount, relevantAsset) {
		// input in satoshis
        def counterparty_sourceAddress = relevantAsset.nativeAddressCounterparty
		def mastercoin_sourceAddress  = relevantAsset.nativeAddressMastercoin
        def blockIdSource = payment.blockIdSource
       
        log4j.info("Processing dividend payment ${payment.blockIdSource} ${payment.txid}. Sending dividend_amount ${dividend_amount} ")
        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)
		
		def mastercoin_tokensOutThere = get_mastercoin_tokens_inuse(relevantAsset)
		def counterparty_tokensOutThere = get_counterparty_tokens_inuse(relevantAsset)
		def totalTokens = mastercoin_tokensOutThere + counterparty_tokensOutThere		
		
		// number of zooz satoshis each one receives
        def quantity_per_share_dividend = Math.round(1.0*dividend_amount/totalTokens)
		
		log4j.info("pay_dividend in counterparty  tokensOutThere = ${counterparty_tokensOutThere} ")
		log4j.info("pay_dividend in mastercoin tokensOutThere = ${mastercoin_tokensOutThere} ")
		log4j.info("Computation: dividend_amount ${dividend_amount} totalTokens ${totalTokens}")
		log4j.info("Quantity per share ${quantity_per_share_dividend}")
		
		// TODO this still gives dividend to cold wallet - issuing address! 

		if (quantity_per_share_dividend == 0) {
			log4j.info("No dividend needed") 
			return
		} 
		
        // Create the (unsigned) counterparty dividend transaction    
        def counterparty_unsignedTransaction = counterpartyAPI.sendDividend(counterparty_sourceAddress, Math.round(quantity_per_share_dividend), relevantAsset.counterpartyAssetName,relevantAsset.counterpartyAssetName)		
        assert counterparty_unsignedTransaction instanceof java.lang.String
        assert counterparty_unsignedTransaction != null
        if (!(counterparty_unsignedTransaction instanceof java.lang.String)) { // catch non technical error in RPC call
            assert counterparty_unsignedTransaction.code == null
        }

        // sign transaction
        def counterparty_signedTransaction = counterpartyAPI.signTransaction(counterparty_unsignedTransaction)
        assert counterparty_signedTransaction instanceof java.lang.String
        assert counterparty_signedTransaction != null

        // send transaction
        try {
            counterpartyAPI.broadcastSignedTransaction(counterparty_signedTransaction)
			if (mastercoin_tokensOutThere > 0 && quantity_per_share_dividend * mastercoin_tokensOutThere > 1.0) { 
				mastercoinAPI.sendDividend(mastercoin_sourceAddress,relevantAsset.mastercoinAssetName, 1.0 * quantity_per_share_dividend * mastercoin_tokensOutThere/satoshi) 
			}
        }
        catch (Throwable e) {
            log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
            db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")			
            assert e == null
        }

        // Lock bitcoin wallet
        bitcoinAPI.lockBitcoinWallet()

        log4j.info("Payment dividend quantity_per_share_dividend ${quantity_per_share_dividend/satoshi} complete")        
    }
	
	public exchange(Long currentBlock, Payment payment) {
	
	}
	
	public pay(Long currentBlock, Payment payment,, BigDecimal outAmount) {
		// input in satoshis
        def sourceAddress = payment.sourceAddress
        def blockIdSource = payment.blockIdSource
        def destinationAddress = payment.destinationAddress
        def asset = payment.outAsset
        
		def amount = Math.round(outAmount)		
		
        log4j.info("Processing payment ${payment.blockIdSource} ${payment.txid}. Sending ${outAmount} ${payment.outAsset} from ${payment.sourceAddress} to ${payment.destinationAddress}")

        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)

		// Native assets are paid via counteraprty as well... 
		if (payment.outAssetType == Asset.COUNTERPARTY_TYPE || payment.outAssetType == Asset.NATIVE_TYPE ) {
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
				log4j.info("update payments set status='complete', lastUpdatedBlockId = ${currentBlock}, outAmount = ${amount} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='complete', lastUpdatedBlockId = ${currentBlock}, outAmount = ${amount} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
			}
			catch (Throwable e) {
				log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				
				assert e == null
			}
		} else {
			// send transaction
			try {
				mastercoinAPI.sendAsset(sourceAddress, destinationAddress, asset, amount/satoshi)
				log4j.info("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
			}
			catch (Throwable e) {
				log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
	
				assert e == null
			}
		}

        // Lock bitcoin wallet
        bitcoinAPI.lockBitcoinWallet()

        log4j.info("Payment ${sourceAddress} -> ${destinationAddress} ${amount} ${asset} complete")        
    }

	// We don't really use the current block... 
    // This is the major thing that needs to be fixed. We shall assume that we have different addresses,
	// so that we can discover... 
	public static int main(String[] args) {
        def paymentProcessor = new PaymentProcessor()
		def Long dividend_percent = 10

        paymentProcessor.init()
        paymentProcessor.audit()

        log4j.info("Payment processor started")
        log4j.info("Last processed payment: " + paymentProcessor.getLastPaymentBlock())
	
        // Begin following blocks
		
		// TODO verify which amounts are satoshi and standardize!!!
        while (true) {
            def blockHeight = bitcoinAPI.getBlockHeight()
            def lastPaymentBlock = paymentProcessor.getLastPaymentBlock()
            def Payment payment = paymentProcessor.getNextPayment()

			// TODO we want to make sure that all followers have finished processing each block !!! 
					
            assert lastPaymentBlock <= blockHeight

            log4j.info("Block ${blockHeight}")

//            // If a payment wasn't performed this block and there is a payment to make
//            if (lastPaymentBlock < blockHeight && payment != null) {
//                paymentProcessor.pay(blockHeight, payment)
//            }
//            if (lastPaymentBlock >= blockHeight && payment != null) {
//                log4j.info("Payment to make but already paid already this block. Sleeping...")
//            }

            if (payment != null) {
				def relevantAsset = paymentProcessor.findAssetConfig(payment)

                if (payment.inAssetType == Asset.NATIVE_TYPE){					
					log4j.info("--------------BUY TRANSACTION-------------")
					// This is an issuing transaction, we need to pay dividend - compute it according to upper margin
					def baseRate = paymentProcessor.computeBaseRate(relevantAsset) 
					def upperMargin = paymentProcessor.computeUpperMargin(relevantAsset,baseRate) 			
					def zoozAmount = upperMargin * (payment.inAmount - getFee(asset)) // The amount of zooz according to the upper margin
					def indexZoozAmount = baseRate * (payment.inAmount - getFee(asset))  // The amount of Zooz according to the index TODO or 1/baseRate? 
					def dividendAmount = indexZoozAmount - zoozAmount // The dividend to be given out
					
					// Pay dividend
					log4j.info("The base rate is ${baseRate}, upper margin is ${upperMargin}")
					log4j.info("Processing payment ${payment.blockIdSource} ${payment.txid} from ${payment.sourceAddress} ")
					log4j.info("Got ${payment.inAmount} BTC, paying out ${zoozAmount} zooz and dividend of ${dividendAmount}")
					paymentProcessor.pay_dividend(blockHeight, payment, dividendAmount,relevantAsset)
					paymentProcessor.pay(blockHeight, payment, zoozAmount)
					
				} else if (payment.outAssetType == Asset.NATIVE_TYPE) {					
					log4j.info("--------------BURN TRANSACTION-------------")					
					def lowerMargin = computeLowerMargin()
					def nativeAmount = (payment.inAmount * lowerMargin) - getFee(asset)

					log4j.info("The base rate is ${baseRate}, lower margin is ${lowerMargin}")
					log4j.info("Processing payment ${payment.blockIdSource} ${payment.txid} from ${payment.sourceAddress} ")
					log4j.info("Got ${payment.inAmount} zooz, paying out ${nativeAmount} BTC")			
					paymentProcessor.pay(blockHeight, payment, nativeAmoubnt)				
					
				} else if ((payment.inAssetType == Asset.MASTERCOIN_TYPE && payment.outAssetType == Asset.COUNTERPARTY_TYPE ) || 				
					(payment.inAssetType == Asset.COUNTERPARTY_TYPE && payment.outAssetType == Asset.MASTERCOIN_TYPE)) {
					
					log4j.info("----------------- EXCHANGE TRANSACTION -----------------")
					log4j.info("The base rate is ${baseRate}, and fee will be reduced")
					log4j.info("Processing payment ${payment.blockIdSource} ${payment.txid} from ${payment.sourceAddress} ")						
					def outAmount = payment,inAmount - getFee(asset) / getBaseExchangeRate(asset)
					paymentProcessor.pay(blockHeight, payment, outAmount)									
				} else {				
					log4j.info("----------------- UNKOWN TRANSACTION TYPE ??? -----------------")
				}				
                log4j.info("Payment complete")
            }
            else {
                log4j.info("No payments to make. Sleeping..${sleepIntervalms}.")
            }

            sleep(sleepIntervalms)
        }

    }
	
	// This gives the reserve fund in satoshis
	private getBTCReserveFund(Asset asset) { 
		def total = 0
		for (address in asset.reserveFundAddresses) {
			total += get_counterparty_asset_balance(address, Asset.NATIVE_TYPE)
		return total
	}	
	
	// The amount of 1 BTC in USD
	private getCurrentBTCBalue() { 
		return currentBTCValueInUSD
	}
	
	// The lower margin is computed as follows: 
	// XL = (reserve fund in BTC) / (Zooz in the world) 
	// then, the key is XL BTC/ZUZ 
	private computeLowerMargin(Asset asset) { 
		return getBTCReserveFund() / getTotalTokensInUse(asset)
	}

	// The upper margin is tricky, we compute it using:
	// XU = baseRate * ( 1 + (mined)/(mined + purchased) OR 
	// XU = 2 XI - XL which is what we'll use  
	private computeUpperMargin(Asset asset, baseRate) {
		return 2 * baseRate - computeLowerMargin(asset)
	}
	
	// Fee is in satoshhis
	private getFee(asset) {
		return asset.txFee * satoshi
	}
	
	// The pegged value in USD of the zooz
	private getValueInUSD(asset) { 
		return asset.valueInUSD
	}

	// The amount of BTC each zooz is currently worth
	private getBaseExchangeRate(asset) { 
		return asset.valueInUSD / getCurrentBTCBalue()
	}
			
}
