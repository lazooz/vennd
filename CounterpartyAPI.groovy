/**
 * Created by whoisjeremylam on 18/04/14.
 * For divisible assets (or, all assets we use) amounts are in satoshis
 */

@Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7' )

import groovyx.net.http.AsyncHTTPBuilder
import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.*

class CounterpartyAPI {
    private String counterpartyTransactionEncoding
    private String counterpartyRpcURL
    private String counterpartyRpcUser
    private String counterpartyRpcPassword
    private groovyx.net.http.AsyncHTTPBuilder counterpartyHttpAsync
    private boolean counterpartyMultisendPerBlock
	static log4j

    private init() {
        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("CounterpartyAPI.ini").toURL())

        counterpartyRpcURL = iniConfig.counterparty.rpcURL
        counterpartyRpcUser = iniConfig.counterparty.rpcUser
        counterpartyRpcPassword = iniConfig.counterparty.rpcPassword
        counterpartyTransactionEncoding = iniConfig.counterparty.counterpartyTransactionEncoding
        counterpartyMultisendPerBlock = iniConfig.counterparty.counterpartyMultisendPerBlock

        counterpartyHttpAsync = new AsyncHTTPBuilder(
                poolSize : 10,
                uri : counterpartyRpcURL,
                contentType : JSON )
        counterpartyHttpAsync.auth.basic counterpartyRpcUser, counterpartyRpcPassword

    }
	
	private sendRPCMessage(command, in_params) {
		def paramsJSON
        def result = counterpartyHttpAsync.request( POST, JSON) { req ->
            body = [method : command,
                    id : 'test',
                    params : in_params,
                    jsonrpc : "2.0"
            ]
		
		paramsJSON = new groovy.json.JsonBuilder(body)
 	    log4j.info(command + " payload: " + paramsJSON)

        response.success = { resp, json ->
            if (json.containsKey("error")) {
                log4j.info(command + " error: " + json.error)
                return json.error
            }
				
            return json.result
        }
	    
 	    response.failure = { resp -> 
			log4j.info(command + " failed: ${response.statusLine}") 
			assert resp.responseBase == null
	    }

		
        }

        assert result instanceof java.util.concurrent.Future
        while ( ! result.done ) {
            Thread.sleep(100)
        }

       return result.get()
	}

    public class Filters {
        def FilterValue[] filters

        public Filters(FilterValue filterValue) {
            filters = new FilterValue[1]
            filters[0]  = filterValue
        }

        public Filters() {
            filters = []
        }

        public Add(FilterValue filterValue) {
            filters.add(filterValue)
        }
    }

    public class FilterValue {
        def String field
        def String op
        def String value

        public FilterValue(String fieldVale, String opValue, String valueValue) {
            field = fieldVale
            op = opValue
            value = valueValue
        }
    }


    public getBalances(address) {
        def filterValue = new FilterValue('address', '==', address)
        def filters = new Filters(filterValue)
		return sendRPCMessage('get_balances', filters)
    }
	
	public getLastBroadcastOfStream(address, datastream_text) { 
		def filterValue = new FilterValue('address', '==', address)
		def secondFilterValue = new FilterValue('text', '==', datastream_text)
        def filters = new Filters(filterValue)
		filters.Add(secondFilterValue)
		return sendRPCMessage('get_broadcasts', ["filters": filters, "limit":1, "orderby": 'timestamp', 'order_dir':'desc'])
	}


    public getSends(Long blockId) {
		return sendRPCMessage('get_sends', ["start_block":blockId,"end_block":blockId])
	}

    public getIssuances(Long blockId) {
		return sendRPCMessage('get_issuances', ["start_block":blockId,"end_block":blockId])
    }

    public broadcastSignedTransaction(String signedTransaction) {
		def result = sendRPCMessage('broadcast_tx', ['signed_tx_hex':signedTransaction])
		if (result == null) { 
			log4j.info("broadcast_tx failed - null was returned")
			assert result != null
		}
		return result
    }

    public signTransaction(String unsignedTransaction) {
		def result = sendRPCMessage('sign_tx', ['unsigned_tx_hex':unsignedTransaction])
		if (result == null) { 
			log4j.info("signTransaction failed - null was returned")
			assert result != null
		}
		return result
    }

    public createSend(sourceAddress, destinationAddress, asset, amount) {
        def myParams

        myParams = ["source":'12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7',"destination":'142UYTzD1PLBcSsww7JxKLck871zRYG5D3',"asset":asset,"quantity":20000]  // in test mode send only just enough for dust

	
		def result = sendRPCMessage('create_send', myParams)
		if (result == null) { 
			log4j.info("create_send failed - null was returned")
			assert result != null
		}
		return result
	}

    public createBroadcast(String sourceAddress, BigDecimal feeFraction, String text, int timestamp, BigDecimal value) {
		def result = sendRPCMessage('create_broadcast', [sourceAddress, feeFraction, text, timestamp, value])
		if (result == null) { 
			log4j.info("create_broadcast failed - null was returned")
			assert result != null
		}
		return result
    }

//    create_issuance(source, asset, quantity, divisible, description
    public createIssuance(sourceAddress, asset, quantity, divisible, description) {
        def result = sendRPCMessage('create_issuance',[sourceAddress, asset, quantity, divisible, description])
		if (result == null) { 
			log4j.info("create_issuance failed - null was returned")
			assert result != null
		}
		return result
	}

	//    create_dividend(source, quantity_per_unit, asset, dividend_asset, encoding='multisig', pubkey=null)
    public sendDividend(sourceAddress, quantity_per_share, asset, dividend_asset) {
	    def myParams
        myParams = [source:sourceAddress, quantity_per_unit:quantity_per_share,asset:asset,dividend_asset:dividend_asset ,encoding:counterpartyTransactionEncoding,multisig_dust_size:counterpartyMultisendPerBlock,op_return_value:null]
		//  myParams = [source: sourceAddress,destination: destinationAddress,asset: asset,quantity: amount, encoding:counterpartyTransactionEncoding, pubkey:null, multisig_dust_size:counterpartyMultisendPerBlock, op_return_value:null]
		def result = sendRPCMessage('create_dividend', myParams)
		if (result == null) { 
			log4j.info("create_dividend failed - null was returned")
			assert result != null
		}
		return result
	}
	
    //get_asset_info(assets)
    public getAssetInfo(asset) {
		return sendRPCMessage('get_asset_info', ['assets':[asset] ] )
	}
 
	// TODO this is not in the API. Why was it written???
    public getBurn() {
		return sendRPCMessage('get_asset_info', [ 'field': 'burned'])
    }

    public CounterpartyAPI(logger) {
        init()
		log4j = logger
    }

}
