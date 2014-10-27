/**
 * Created by amirza on 19/07/14.
 */

public class Asset {
	public static String MASTERCOIN_TYPE = "MSC"
	public static String COUNTERPARTY_TYPE = "XCP"
	public static String NATIVE_TYPE = "BTC"

    public String counterpartyAssetName
	public String mastercoinAssetName
    public String nativeAssetName
	
    public String counterpartyAddress // the counterparty/bitcoin address side where we will receive the Counterparty asset
	public String mastercoinAddress // the mastercoin/bitcoin address side where we will receive the mastercoin asset
    public String nativeAddressCounterparty // The address which users should send their native asset to get counterparty
	public String nativeAddressMastercoin // The address which users should send their native asset to get mastercoin
	public String counterpartyToMastercoinAddress // The address for a counterparty -> mastercoin conversion
	public String mastercoinToCounterpartyAddress // The address for a mastercoin -> counterparty conversion
	
	public reserveFundAddresses 			// The addresses of the reserve fund
	public BigDecimal valueInUSD			// The value of the asset in USD

    public BigDecimal txFee
    public BigDecimal feePercentage
    public boolean mappingRequired
    public issuanceDependent
    public issuanceSource
    public issuanceAsset
    public issuanceDivisible
    public issuanceDescription

    public Asset(String counterpartyAssetNameValue, String mastercoinAssetNameValue, String nativeAssetNameValue, String counterpartyAddressValue, String mastercoinAddressValue, String nativeAddressCounterpartyValue, String nativeAddressMastercoinValue, String counterpartyToMastercoinAddressValue, String mastercoinToCounterpartyAddressValue,BigDecimal txFeeValue, BigDecimal feePercentageValue, boolean mappingRequiredValue, boolean issuanceDependentValue, String issuanceSourceValue, String issuanceAssetValue, boolean issuanceDivisibleValue, String issuanceDescriptionValue, reserveFundAddresesValue, BigDecimal valueInUSDValue) {
       
	    counterpartyAssetName = counterpartyAssetNameValue
		mastercoinAssetName  = mastercoinAssetNameValue
		nativeAssetName = nativeAssetNameValue
		
        counterpartyAddress = counterpartyAddressValue
		mastercoinAddress = mastercoinAddressValue
		nativeAddressCounterparty = nativeAddressCounterpartyValue
		nativeAddressMastercoin = nativeAddressMastercoinValue
		counterpartyToMastercoinAddress = counterpartyToMastercoinAddressValue
		mastercoinToCounterpartyAddress = mastercoinToCounterpartyAddressValue	       
		        
        txFee = txFeeValue
        feePercentage = feePercentageValue
        mappingRequired = mappingRequiredValue
        issuanceDependent = issuanceDependentValue
        issuanceSource = issuanceSourceValue
        issuanceAsset = issuanceAssetValue
        issuanceDivisible = issuanceDivisibleValue
        issuanceDescription = issuanceDescriptionValue
		
		valueInUSD = valueInUSDValue
		reserveFundAddresses =  reserveFundAddressesValue
    }
	
	public static readAssets(String file) {
		def iniConfig = new ConfigSlurper().parse(new File(file).toURL())
		
		def assetConfig = []
        iniConfig.asset.each { it ->
            def issuanceDependent
            def issuanceSource
            def issuanceAsset
            def issuanceDivisible
            def issuanceDescription

            if (it.value.issuanceDependent instanceof groovy.util.ConfigObject) { issuanceDependent = false} else {issuanceDependent = it.value.issuanceDependent}
            if (it.value.issuanceSource instanceof groovy.util.ConfigObject) { issuanceSource = ""} else {issuanceSource = it.value.issuanceSource}
            if (it.value.issuanceAsset instanceof groovy.util.ConfigObject) { issuanceAsset = ""} else {issuanceAsset = it.value.issuanceAsset}
            if (it.value.issuanceDivisible instanceof groovy.util.ConfigObject) { issuanceDivisible = true} else {issuanceDivisible = it.value.issuanceDivisible}
            if (it.value.issuanceDescription instanceof groovy.util.ConfigObject) { issuanceDescription = ""} else {issuanceDescription = it.value.issuanceDescription}

            def currentAsset = new Asset(it.value.counterpartyAssetName, it.value.mastercoinAssetName, it.value.nativeAssetName, it.value.counterpartyAddress, it.value.mastercoinAddress, it.value.nativeAddressCounterparty , it.value.nativeAddressMastercoin, it.value.counterpartyToMastercoinAddress,it.value.mastercoinToCounterpartyAddress, it.value.txFee, it.value.feePercentage, it.value.mappingRequired, issuanceDependent, issuanceSource, issuanceAsset, issuanceDivisible, issuanceDescription, it.value.reserveFundAddresses, it.value.valueInUSD)
            assetConfig.add(currentAsset)
        }
		
		return assetConfig
	}
}
