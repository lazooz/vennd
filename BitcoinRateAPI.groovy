import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.Method.GET
import groovyx.net.http.AsyncHTTPBuilder

@Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7' )

/**
 * Created by whoisjeremylam on 18/04/14.
 */
class BitcoinRateAPI {	
	private httpBuilders = []
	private fields = []    
   
    private getQueryResult(httpAsync) {
        def result = httpAsync.request( GET, JSON) { req ->
		
            response.success = { resp, json ->				
                return json
            }
        }

        assert result instanceof java.util.concurrent.Future
        while ( ! result.done ) {
            Thread.sleep(100)
        }
		
        return result.get()
    }

    private init(String iniFile) {
        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File(iniFile).toURL())
		
		iniConfig.sites.each { it -> 
	        // Init async http handler
			def httpAsync = new AsyncHTTPBuilder(
                poolSize : 10,
                uri : it.value.siteUrl,,
                contentType : JSON )			
			httpBuilders.push(httpAsync)
			fields.push(it.value.field)			
		}
    }
	
	public getAveragedRate() { 
		def numResults = 0.0
		def total = 0.0
		for (def i = 0; i < httpBuilders.size(); i++) { 
			def result = getQueryResult(httpBuilders[i])
			def fieldList = fields[i]			
			for (field in fieldList) {
				if (result == null) continue
				result = result[field]				
			}						
			numResults += 1
			total += result.toFloat()
		}
		return total / numResults
	}

    public BitcoinRateAPI() {
        init("BitcoinRateAPI.ini")
    }

    public BitcoinRateAPI(String iniFile) {
        init(iniFile)		
    }

	public static int main(String[] args) {
        def bitcoinRateAPI = new BitcoinRateAPI()
		def result = bitcoinRateAPI.getAveragedRate()
		println "Average: ${result}"
		
		// every 20 secs
		def exchangeRateUpdateRate = 5 *  1000
		
		Timer timer = new Timer()
		timer.scheduleAtFixedRate(new BTCUSDRateUpdateTask(), exchangeRateUpdateRate, exchangeRateUpdateRate)
		
		return 0
	}
	
	
	static class BTCUSDRateUpdateTask extends TimerTask { 
		public void run() {
			def bitcoinRateAPI = new BitcoinRateAPI()
			def currentBTCValueInUSD = bitcoinRateAPI.getAveragedRate()
			println "Updated exchange rate is: ${currentBTCValueInUSD} USD for 1 BTC"
		}
	}
}
