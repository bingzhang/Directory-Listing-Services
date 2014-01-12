package stork.dls.stream;

public class DLSProxyInfo {
	int credport = -1;
	String proxyserver; 
	String account;
	String passphrase;
	private DLSProxyInfo(){
		
	}
	public static DLSProxyInfo createDLSProxy(String proxyserver, String account, String passphrase){
		if(null == account || account.isEmpty() ||null == passphrase || passphrase.isEmpty()){
			return null;
		}
		return new DLSProxyInfo(proxyserver, account, passphrase);
	}
	private DLSProxyInfo(String proxyserver, String account, String passphrase){
		this.account = account;
		this.passphrase = passphrase;
		this.proxyserver = proxyserver;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
