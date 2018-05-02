package stork.dls.config;

/**
 * 
 * @author bing
 *
 */
public class DLSConfig {
    public static boolean TCPNODELAY = false;//true
    public static boolean CC_LISTING = true;
    public static boolean SINGLETON = true;
    public static String REPLICA_QUEUE_HOST = "localhost";
  	public static int DLS_PIPE_CAPACITY = 1;//1;5;
  	public static int DLS_CONCURRENCY_STREAM = 50;//1;//50;
  	public static int DLS_MAX_THREADS = 1000;
    public static String RABBITMQ_URI = "";
    public static boolean DLSEDGE = true;
}