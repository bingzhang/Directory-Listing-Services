package org.irods.jargon.testutils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;

/**
 * Utilities to load testing properties from a properties file
 * 
 * @author Mike Conway, DICE (www.irods.org)
 * @since 10/18/2009
 */
public class TestingPropertiesHelper {
	public static final String IRODS_GSI_HOST_KEY = "test.option.gsi.host";
	public static final String IRODS_GSI_PORT_KEY = "test.option.gsi.port";
	public static final String IRODS_GSI_ZONE_KEY = "test.option.gsi.zone";
	public static final String IRODS_GSI_DN_KEY = "test.option.gsi.dn";
	public static final String IRODS_GSI_CERT_PATH = "test.option.gsi.file";
	public static String GENERATED_FILE_DIRECTORY_KEY = "test.data.directory";
	public static String IRODS_USER_KEY = "test.irods.user";
	public static String IRODS_PASSWORD_KEY = "test.irods.password";
	public static String IRODS_RESOURCE_KEY = "test.irods.resource";
	public static String IRODS_USER_DN_KEY = "test.irods.userDN";
	public static String IRODS_SECONDARY_USER_KEY = "test2.irods.user";
	public static String IRODS_SECONDARY_PASSWORD_KEY = "test2.irods.password";
	public static String IRODS_SECONDARY_RESOURCE_KEY = "test2.irods.resource";
	public static String IRODS_TERTIARY_USER_KEY = "test3.irods.user";
	public static String IRODS_TERTIARY_PASSWORD_KEY = "test3.irods.password";
	public static String IRODS_TERTIARY_RESOURCE_KEY = "test3.irods.resource";
	public static String IRODS_HOST_KEY = "test.irods.host";
	public static String IRODS_PORT_KEY = "test.irods.port";
	public static String IRODS_ZONE_KEY = "test.irods.zone";
	public static String IRODS_SCRATCH_DIR_KEY = "test.irods.scratch.subdir";
	public static String IRODS_CONFIRM_TESTING_KEY = "test.confirm";
	public static String IRODS_ADMIN_USER_KEY = "test.irods.admin";
	public static String IRODS_ADMIN_PASSWORD_KEY = "test.irods.admin.password";
	public static String IRODS_RESOURCE_GROUP_KEY = "test.resource.group";
	public static String IRODS_USER_GROUP_KEY = "jargon.test.user.group";
	public static String IRODS_TEST_FEDERATED_ZONE_KEY = "test.option.federated.zone";
	public static String IRODS_TEST_OPTION_KERBEROS_KEY = "test.option.kerberos";
	public static String IRODS_FEDERATED_HOST_KEY = "test.federated.irods.host";
	public static String IRODS_FEDERATED_PORT_KEY = "test.federated.irods.port";
	public static String IRODS_FEDERATED_ZONE_KEY = "test.federated.irods.zone";
	public static String IRODS_FEDERATED_RESOURCE_KEY = "test.federated.irods.resource";
	public static String IRODS_FEDERATED_USER_KEY = "test.federated.irods.user";
	public static String IRODS_FEDERATED_PASSWORD_KEY = "test.federated.irods.password";
	public static String IRODS_FEDERATED_ADMIN_USER_KEY = "test.federated.irods.admin";
	public static String IRODS_FEDERATED_ADMIN_PASSWORD_KEY = "test.federated.irods.admin.password";
	public static String IRODS_KERBEROS_USER_KEY = "jargon.test.kerberos.user";
	public static String IRODS_TEST_OPTION_PAM_KEY = "test.option.pam";
	public static String IRODS_PAM_USER_KEY = "jargon.test.pam.user";
	public static String IRODS_PAM_PASSWORD_KEY = "jargon.test.pam.password";
	public static String IRODS_CONFIRM_TESTING_TRUE = "true";
	public static String IRODS_CONFIRM_TESTING_FALSE = "false";
	public static String IRODS_TEST_OPTION_EIRODS = "test.option.eirods";

	/**
	 * Return the given property (by key) as an int
	 * 
	 * @param testingProperties
	 * @param key
	 * @return
	 * @throws TestingUtilsException
	 */
	public int getPropertyValueAsInt(final Properties testingProperties,
			final String key) throws TestingUtilsException {
		String propVal = (String) testingProperties.get(key);

		if (propVal == null || propVal.length() == 0) {
			throw new TestingUtilsException(
					"missing or invalid value in testing.properties");
		}

		int retVal = 0;

		try {
			retVal = Integer.parseInt(propVal);
		} catch (NumberFormatException nfe) {
			throw new TestingUtilsException(
					"port is in valid format to convert to int:" + propVal, nfe);
		}

		return retVal;
	}

	/**
	 * Get the standard iRODS test server port from the testing properties
	 * 
	 * @param testingProperties
	 * @return
	 * @throws TestingUtilsException
	 */
	public int getPortAsInt(final Properties testingProperties)
			throws TestingUtilsException {
		return getPropertyValueAsInt(testingProperties,
				TestingPropertiesHelper.IRODS_PORT_KEY);
	}

	/**
	 * Load the properties that control various tests from the
	 * testing.properties file on the code path
	 * 
	 * @return <code>Properties</code> class with the test values
	 * @throws TestingUtilsException
	 */
	public Properties getTestProperties() throws TestingUtilsException {
		ClassLoader loader = this.getClass().getClassLoader();
		InputStream in = loader.getResourceAsStream("testing.properties");
		Properties properties = new Properties();

		try {
			properties.load(in);
		} catch (IOException ioe) {
			throw new TestingUtilsException("error loading test properties",
					ioe);
		} finally {
			try {
				in.close();
			} catch (Exception e) {
				// ignore
			}
		}

		String testingConfirm = properties
				.getProperty(IRODS_CONFIRM_TESTING_KEY);
		if (testingConfirm == null) {
			throw new TestingUtilsException(
					"did not find a test.confirm property in testing.properties");
		} else if (!testingConfirm.equals(IRODS_CONFIRM_TESTING_TRUE)) {
			throw new TestingUtilsException(
					"test.confirm property in testing.properties needs to be set to true");
		}

		return properties;
	}

	/**
	 * Get a URI in IRODS format that points to a scratch file, given the file
	 * name and any additional path to that file without a leading '/'. For
	 * example:
	 * 
	 * Given that I have a file under the irods collection
	 * /test1/home/test/test-scratch/an_irods_subdir/file.txt
	 * 
	 * I can construct a proper URI like this:
	 * 
	 * <pre>
	 * 
	 * 
	 * StringBuilder uriPath = new StringBuilder();
	 * uriPath.append(&quot;an_irods_subdir&quot;);
	 * uriPath.append('/');
	 * uriPath.append(file.txt);
	 * 
	 * URI irodsUri = testingPropertiesHelper
	 * 		.buildUriFromTestPropertiesForFileInUserDir(testingProperties,
	 * 				uriPath.toString());
	 * 
	 * 
	 * </pre>
	 * 
	 * Note that the scratch directory and everything above it is computed from
	 * testing.properties
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @param fileName
	 *            <code>String</code> with the path (no leading '/') below the
	 *            user scratch directory as defined in testing.properties
	 * @return <code>URI</code>
	 * @throws URISyntaxException
	 */
	public URI buildUriFromTestPropertiesForFileInUserDir(
			final Properties testingProperties, final String fileName)
			throws URISyntaxException {
		StringBuilder irodsUri = new StringBuilder();

		irodsUri.append("irods://");
		irodsUri.append(testingProperties.getProperty(IRODS_USER_KEY));
		irodsUri.append(".");
		irodsUri.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		irodsUri.append(":");
		irodsUri.append(testingProperties.getProperty(IRODS_PASSWORD_KEY));
		irodsUri.append("@");
		irodsUri.append(testingProperties.getProperty(IRODS_HOST_KEY));
		irodsUri.append(":");
		irodsUri.append(String.valueOf(testingProperties
				.getProperty(IRODS_PORT_KEY)));
		irodsUri.append("/");
		irodsUri.append(testingProperties.get(IRODS_ZONE_KEY));
		irodsUri.append("/home/");
		irodsUri.append(testingProperties.get(IRODS_USER_KEY));
		irodsUri.append("/");
		irodsUri.append(testingProperties.getProperty(IRODS_SCRATCH_DIR_KEY));
		irodsUri.append("/");
		irodsUri.append(fileName);

		return new URI(irodsUri.toString());
	}

	/**
	 * Get a URI in IRODS format that points to a scratch file, given the file
	 * name and any additional path to that file without a leading '/'. This
	 * version does not place the password into the URI For example:
	 * 
	 * Given that I have a file under the irods collection
	 * /test1/home/test/test-scratch/an_irods_subdir/file.txt
	 * 
	 * I can construct a proper URI like this:
	 * 
	 * <pre>
	 * 
	 * 
	 * StringBuilder uriPath = new StringBuilder();
	 * uriPath.append(&quot;an_irods_subdir&quot;);
	 * uriPath.append('/');
	 * uriPath.append(file.txt);
	 * 
	 * URI irodsUri = testingPropertiesHelper
	 * 		.buildUriFromTestPropertiesForFileInUserDir(testingProperties,
	 * 				uriPath.toString());
	 * 
	 * 
	 * </pre>
	 * 
	 * Note that the scratch directory and everything above it is computed from
	 * testing.properties
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @param fileName
	 *            <code>String</code> with the path (no leading '/') below the
	 *            user scratch directory as defined in testing.properties
	 * @return <code>URI</code>
	 * @throws URISyntaxException
	 */
	public URI buildUriFromTestPropertiesForFileInUserDirNoPasswordOrZone(
			final Properties testingProperties, final String fileName)
			throws URISyntaxException {
		StringBuilder irodsUri = new StringBuilder();

		irodsUri.append("irods://");
		irodsUri.append(testingProperties.getProperty(IRODS_USER_KEY));
		irodsUri.append("@");
		irodsUri.append(testingProperties.getProperty(IRODS_HOST_KEY));
		irodsUri.append(":");
		irodsUri.append(String.valueOf(testingProperties
				.getProperty(IRODS_PORT_KEY)));
		irodsUri.append("/");
		irodsUri.append(testingProperties.get(IRODS_ZONE_KEY));
		irodsUri.append("/home/");
		irodsUri.append(testingProperties.get(IRODS_USER_KEY));
		irodsUri.append("/");
		irodsUri.append(testingProperties.getProperty(IRODS_SCRATCH_DIR_KEY));
		irodsUri.append("/");
		irodsUri.append(fileName);

		return new URI(irodsUri.toString());
	}

	/**
	 * Get a URI in IRODS format that points to a scratch file, given the file
	 * name and any additional path to that file without a leading '/'.
	 * <p/>
	 * This version will not include user information.
	 * <p/>
	 * 
	 * Note that the scratch directory and everything above it is computed from
	 * testing.properties
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @param fileName
	 *            <code>String</code> with the path (no leading '/') below the
	 *            user scratch directory as defined in testing.properties
	 * @return <code>URI</code>
	 * @throws URISyntaxException
	 */
	public URI buildUriFromTestPropertiesForFileNoUserInfo(
			final Properties testingProperties, final String fileName)
			throws URISyntaxException {
		StringBuilder irodsUri = new StringBuilder();

		irodsUri.append("irods://");
		irodsUri.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		irodsUri.append("@");
		irodsUri.append(testingProperties.getProperty(IRODS_HOST_KEY));
		irodsUri.append(":");
		irodsUri.append(String.valueOf(testingProperties
				.getProperty(IRODS_PORT_KEY)));
		irodsUri.append("/");
		irodsUri.append(testingProperties.get(IRODS_ZONE_KEY));
		irodsUri.append("/home/");
		irodsUri.append(testingProperties.get(IRODS_USER_KEY));
		irodsUri.append("/");
		irodsUri.append(testingProperties.getProperty(IRODS_SCRATCH_DIR_KEY));
		irodsUri.append("/");
		irodsUri.append(fileName);

		return new URI(irodsUri.toString());
	}

	/**
	 * Create a URI compatable with a local <code>IRODSFileImpl</code> based on
	 * a set of peroperties, as well as a relative path (no leading '/')
	 * underneath the users local scratch directory as defined in
	 * testing.properties.
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> defined in the testing.properties file
	 * @param fileName
	 *            <code>String</code> containing the relative path (no leading
	 *            '/') underneath the local scratch directory. Note that the
	 *            subdirectories will be created if they do not aleady exist.
	 * @return <code>URI</code> to a local file in a format that can be used
	 *         with an <code>IRODSFileImpl</code>
	 * @throws URISyntaxException
	 */
	public URI buildUriFromTestPropertiesForFileInLocalScratchDir(
			final Properties testingProperties, final String fileName)
			throws URISyntaxException {
		StringBuilder irodsUri = new StringBuilder();

		irodsUri.append("file://");
		File scratchDir = new File(
				testingProperties.getProperty(GENERATED_FILE_DIRECTORY_KEY)
						+ fileName);
		scratchDir.mkdirs();
		irodsUri.append(scratchDir.getAbsolutePath());

		return new URI(irodsUri.toString());
	}

	/**
	 * Get a URI in IRODS format that points to a scratch file, given the file
	 * name and any additional path to that file without a leading '/', for the
	 * secondary testing user. For example:
	 * 
	 * Note that the scratch directory and everything above it is computed from
	 * testing.properties
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @param fileName
	 *            <code>String</code> with the path (no leading '/') below the
	 *            user scratch directory as defined in testing.properties
	 * @return <code>URI</code>
	 * @throws URISyntaxException
	 */
	public URI buildUriFromTestPropertiesForFileInSecondaryUserDir(
			final Properties testingProperties, final String fileName)
			throws URISyntaxException {
		StringBuilder irodsUri = new StringBuilder();

		irodsUri.append("irods://");
		irodsUri.append(testingProperties.getProperty(IRODS_SECONDARY_USER_KEY));
		irodsUri.append(".");
		irodsUri.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		irodsUri.append(":");
		irodsUri.append(testingProperties
				.getProperty(IRODS_SECONDARY_PASSWORD_KEY));
		irodsUri.append("@");
		irodsUri.append(testingProperties.getProperty(IRODS_HOST_KEY));
		irodsUri.append(":");
		irodsUri.append(String.valueOf(testingProperties
				.getProperty(IRODS_PORT_KEY)));
		irodsUri.append("/");
		irodsUri.append(testingProperties.get(IRODS_ZONE_KEY));
		irodsUri.append("/home/");
		irodsUri.append(testingProperties.get(IRODS_SECONDARY_USER_KEY));
		irodsUri.append("/");
		irodsUri.append(testingProperties.getProperty(IRODS_SCRATCH_DIR_KEY));
		irodsUri.append("/");
		irodsUri.append(fileName);

		return new URI(irodsUri.toString());
	}

	/**
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount}
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildIRODSAdminAccountFromTestProperties(
			final Properties testingProperties) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ADMIN_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_HOST_KEY),
				Integer.parseInt(testingProperties.getProperty(IRODS_PORT_KEY)),
				testingProperties.getProperty(IRODS_ADMIN_USER_KEY),
				testingProperties.getProperty(IRODS_ADMIN_PASSWORD_KEY),
				homeBuilder.toString(), testingProperties
						.getProperty(IRODS_ZONE_KEY), testingProperties
						.getProperty(IRODS_RESOURCE_KEY));

		return account;
	}

	/**
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount}
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildIRODSAccountFromTestProperties(
			final Properties testingProperties) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_HOST_KEY),
				Integer.parseInt(testingProperties.getProperty(IRODS_PORT_KEY)),
				testingProperties.getProperty(IRODS_USER_KEY),
				testingProperties.getProperty(IRODS_PASSWORD_KEY), homeBuilder
						.toString(), testingProperties
						.getProperty(IRODS_ZONE_KEY), testingProperties
						.getProperty(IRODS_RESOURCE_KEY));

		return account;
	}

	/**
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount} for anonymous access
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildAnonymousIRODSAccountFromTestProperties(
			final Properties testingProperties) throws JargonException {

		IRODSAccount account = IRODSAccount
				.instanceForAnonymous(testingProperties
						.getProperty(IRODS_HOST_KEY),
						Integer.parseInt(testingProperties
								.getProperty(IRODS_PORT_KEY)), "",
						testingProperties.getProperty(IRODS_ZONE_KEY),
						testingProperties.getProperty(IRODS_RESOURCE_KEY));

		return account;
	}

	/**
	 * Build an <code>IRODSAccount</code> object for connecting to the federated
	 * zone as defined in the testing.properties file.
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount}
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildIRODSAccountForFederatedZoneFromTestProperties(
			final Properties testingProperties) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties
				.getProperty(IRODS_FEDERATED_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties
				.getProperty(IRODS_FEDERATED_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_FEDERATED_HOST_KEY),
				Integer.parseInt(testingProperties
						.getProperty(IRODS_FEDERATED_PORT_KEY)),
				testingProperties.getProperty(IRODS_FEDERATED_USER_KEY),
				testingProperties.getProperty(IRODS_FEDERATED_PASSWORD_KEY),
				homeBuilder.toString(),
				testingProperties.getProperty(IRODS_FEDERATED_ZONE_KEY),
				testingProperties.getProperty(IRODS_FEDERATED_RESOURCE_KEY));

		return account;
	}

	/**
	 * Build a test account where the default resource will be set to blank
	 * 
	 * @param testingProperties
	 * @return
	 */
	public IRODSAccount buildIRODSAccountFromTestPropertiesWithBlankResource(
			final Properties testingProperties) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_HOST_KEY),
				Integer.parseInt(testingProperties.getProperty(IRODS_PORT_KEY)),
				testingProperties.getProperty(IRODS_USER_KEY),
				testingProperties.getProperty(IRODS_PASSWORD_KEY), homeBuilder
						.toString(), testingProperties
						.getProperty(IRODS_ZONE_KEY), "");

		return account;
	}

	/**
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @param userName
	 *            <code>String</code> user name which will be used for the
	 *            account (will not automatically set the user from testing
	 *            properties).
	 * @param password
	 *            <code>String</code> with the password to be used for the
	 *            account.
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount}
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildIRODSAccountForIRODSUserFromTestPropertiesForGivenUser(
			final Properties testingProperties, final String userName,
			final String password) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_HOST_KEY),
				Integer.parseInt(testingProperties.getProperty(IRODS_PORT_KEY)),
				userName, password, homeBuilder.toString(), testingProperties
						.getProperty(IRODS_ZONE_KEY), testingProperties
						.getProperty(IRODS_RESOURCE_KEY));

		return account;
	}

	/**
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount}
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildIRODSAccountFromSecondaryTestProperties(
			final Properties testingProperties) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties
				.getProperty(IRODS_SECONDARY_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_HOST_KEY),
				Integer.parseInt(testingProperties.getProperty(IRODS_PORT_KEY)),
				testingProperties.getProperty(IRODS_SECONDARY_USER_KEY),
				testingProperties.getProperty(IRODS_SECONDARY_PASSWORD_KEY),
				homeBuilder.toString(), testingProperties
						.getProperty(IRODS_ZONE_KEY), testingProperties
						.getProperty(IRODS_SECONDARY_RESOURCE_KEY));

		return account;
	}

	/**
	 * @param testingProperties
	 *            <code>Properties</code> file with the standard names defined
	 *            in
	 *            {@link org.TestingPropertiesHelper.jargon.test.utils.TestingPropertiesLoader}
	 * @return @link{ edu.sdsc.grid.io.irods.IRODSAccount}
	 * @throws URISyntaxException
	 */
	public IRODSAccount buildIRODSAccountFromTertiaryTestProperties(
			final Properties testingProperties) {

		StringBuilder homeBuilder = new StringBuilder();
		homeBuilder.append('/');
		homeBuilder.append(testingProperties.getProperty(IRODS_ZONE_KEY));
		homeBuilder.append('/');
		homeBuilder.append("home");
		homeBuilder.append('/');
		homeBuilder.append(testingProperties
				.getProperty(IRODS_TERTIARY_USER_KEY));

		IRODSAccount account = new IRODSAccount(
				testingProperties.getProperty(IRODS_HOST_KEY),
				Integer.parseInt(testingProperties.getProperty(IRODS_PORT_KEY)),
				testingProperties.getProperty(IRODS_TERTIARY_USER_KEY),
				testingProperties.getProperty(IRODS_TERTIARY_PASSWORD_KEY),
				homeBuilder.toString(), testingProperties
						.getProperty(IRODS_ZONE_KEY), testingProperties
						.getProperty(IRODS_TERTIARY_RESOURCE_KEY));

		return account;
	}

	/**
	 * Handy method to give, from the root IRODS collection, a full path to a
	 * given collection in the IRODS test scratch area on IRODS
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> that define test behavior
	 * @param collectionPathBelowScratch
	 *            <code>String</code> with no leading '/' that defines the
	 *            desired path underneath the IRODS scratch directory
	 * @return <code>String</code> with trailing '/' that gives the absolute
	 *         path for an IRODS collection
	 * @throws TestingUtilsException
	 * @throws URISyntaxException
	 */
	public String buildIRODSCollectionAbsolutePathFromTestProperties(
			final Properties testingProperties,
			final String collectionPathBelowScratch)
			throws TestingUtilsException {

		if (testingProperties.get(IRODS_SCRATCH_DIR_KEY) == null) {
			throw new TestingUtilsException(
					"scratch path not provided in testing.properties");
		}

		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_ZONE_KEY));
		pathBuilder.append("/home/");
		pathBuilder.append(testingProperties.get(IRODS_USER_KEY));
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_SCRATCH_DIR_KEY));
		pathBuilder.append('/');
		pathBuilder.append(collectionPathBelowScratch);
		return pathBuilder.toString();
	}

	/**
	 * Handy method to give, from the root IRODS collection, a full path to a
	 * given collection in the IRODS test scratch area on IRODS for the rods
	 * user
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> that define test behavior
	 * @param collectionPathBelowScratch
	 *            <code>String</code> with no leading '/' that defines the
	 *            desired path underneath the IRODS scratch directory
	 * @return <code>String</code> with trailing '/' that gives the absolute
	 *         path for an IRODS collection
	 * @throws TestingUtilsException
	 * @throws URISyntaxException
	 */
	public String buildIRODSCollectionAbsolutePathFromTestPropertiesForRods(
			final Properties testingProperties,
			final String collectionPathBelowScratch)
			throws TestingUtilsException {

		if (testingProperties.get(IRODS_SCRATCH_DIR_KEY) == null) {
			throw new TestingUtilsException(
					"scratch path not provided in testing.properties");
		}

		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_ZONE_KEY));
		pathBuilder.append("/home/");
		pathBuilder.append("rods");
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_SCRATCH_DIR_KEY));
		pathBuilder.append('/');
		pathBuilder.append(collectionPathBelowScratch);
		return pathBuilder.toString();
	}

	/**
	 * Handy method to give, from the root IRODS collection, a full path to a
	 * given collection in the IRODS test scratch area on IRODS on the
	 * configured federated zone.
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> that define test behavior
	 * @param collectionPathBelowScratch
	 *            <code>String</code> with no leading '/' that defines the
	 *            desired path underneath the IRODS scratch directory
	 * @return <code>String</code> with trailing '/' that gives the absolute
	 *         path for an IRODS collection
	 * @throws TestingUtilsException
	 * @throws URISyntaxException
	 */
	public String buildIRODSCollectionAbsolutePathFromFederatedZoneReadTestProperties(
			final Properties testingProperties,
			final String collectionPathBelowScratch)
			throws TestingUtilsException {

		if (testingProperties.get(IRODS_SCRATCH_DIR_KEY) == null) {
			throw new TestingUtilsException(
					"scratch path not provided in testing.properties");
		}

		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_FEDERATED_ZONE_KEY));
		pathBuilder.append("/home/");
		pathBuilder.append(testingProperties.get(IRODS_FEDERATED_USER_KEY));
		pathBuilder.append("/fedread/");
		pathBuilder.append(testingProperties.get(IRODS_SCRATCH_DIR_KEY));
		pathBuilder.append('/');
		pathBuilder.append(collectionPathBelowScratch);
		return pathBuilder.toString();
	}

	/**
	 * Handy method to give, from the root IRODS collection, a full path to a
	 * given collection in the IRODS test scratch area on IRODS
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> that define test behavior
	 * @param collectionPathBelowScratch
	 *            <code>String</code> with no leading '/' that defines the
	 *            desired path underneath the IRODS scratch directory
	 * @return <code>String</code> with trailing '/' that gives the absolute
	 *         path for an IRODS collection
	 * @throws TestingUtilsException
	 * @throws URISyntaxException
	 */
	public String buildIRODSCollectionAbsolutePathFromSecondaryTestProperties(
			final Properties testingProperties,
			final String collectionPathBelowScratch)
			throws TestingUtilsException {

		if (testingProperties.get(IRODS_SCRATCH_DIR_KEY) == null) {
			throw new TestingUtilsException(
					"scratch path not provided in testing.properties");
		}

		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_ZONE_KEY));
		pathBuilder.append("/home/");
		pathBuilder.append(testingProperties.get(IRODS_SECONDARY_USER_KEY));
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_SCRATCH_DIR_KEY));
		pathBuilder.append('/');
		pathBuilder.append(collectionPathBelowScratch);
		return pathBuilder.toString();
	}

	/**
	 * Handy method to give, from the root IRODS collection, a relative path
	 * under the home directory for the described user
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> that define test behavior
	 * @param collectionPathBelowScratch
	 *            <code>String</code> with no leading '/' that defines the
	 *            desired path underneath the IRODS scratch directory
	 * @return <code>String</code> with trailing '/' that gives the absolute
	 *         path for an IRODS collection
	 * @throws URISyntaxException
	 */
	public String buildIRODSCollectionRelativePathFromTestProperties(
			final Properties testingProperties,
			final String collectionPathBelowScratch) {
		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append(testingProperties.get(IRODS_SCRATCH_DIR_KEY));
		pathBuilder.append('/');
		pathBuilder.append(collectionPathBelowScratch);
		return pathBuilder.toString();
	}

	/**
	 * Check if optional distributed resources are to be tested
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestDistributedResources(final Properties testingProperties) {
		String val = (String) testingProperties
				.get("test.option.distributed.resources");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional kerberos testing to be done
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestKerberos(final Properties testingProperties) {
		String val = (String) testingProperties.get("test.option.kerberos");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional registration functions are to be tested (tests must be
	 * running on a resource server)
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestRegistration(final Properties testingProperties) {
		String val = (String) testingProperties.get("test.option.registration");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional remote stream execution is to be tested
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestRemoteExecStream(final Properties testingProperties) {
		String val = (String) testingProperties
				.get("test.option.exercise.remoteexecstream");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional audit system is to be tested
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestAudit(final Properties testingProperties) {
		String val = (String) testingProperties
				.get("test.option.exercise.audit");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional ticket system is to be tested
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestTickets(final Properties testingProperties) {
		String val = (String) testingProperties
				.get("test.option.exercise.ticket");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional gsi auth system is to be tested
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestGSI(final Properties testingProperties) {
		String val = (String) testingProperties.get("test.option.gsi");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if optional pam auth system is to be tested
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestPAM(final Properties testingProperties) {
		String val = (String) testingProperties.get("test.option.pam");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if tests depending on a federated zone should be run. The federated
	 * zone must be configured as described in the test-scripts/fedTestSetup.txt
	 * file.
	 * 
	 * @param testingProperties
	 * @return <code>true</code> if the federated zone testing should occur
	 */
	public boolean isTestFederatedZone(final Properties testingProperties) {
		String val = (String) testingProperties
				.get(IRODS_TEST_FEDERATED_ZONE_KEY);
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Check if tests depending on strictACL's should be run. This requires the
	 * configuration of the iRODS core.re for strictACL, as well as the
	 * provisioning of a /zone/home/public directory with anonymous rights
	 * 
	 * @param testingProperties
	 * @return <code>true</code> if strict ACL testing should occur
	 */
	public boolean isTestStrictACL(final Properties testingProperties) {
		String val = (String) testingProperties.get("test.option.strictACL");
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Are we testing eirods?
	 * 
	 * @param testingProperties
	 * @return
	 */
	public boolean isTestEirods(final Properties testingProperties) {
		String val = (String) testingProperties
				.get(TestingPropertiesHelper.IRODS_TEST_OPTION_EIRODS);
		if (val == null) {
			return false;
		} else {
			return Boolean.parseBoolean(val);
		}
	}

	/**
	 * Handy method to give, from the root IRODS collection, a full path to a
	 * given collection in the IRODS test scratch area on IRODS on the
	 * configured federated zone enabled for writing cross-zone.
	 * 
	 * @param testingProperties
	 *            <code>Properties</code> that define test behavior
	 * @param collectionPathBelowScratch
	 *            <code>String</code> with no leading '/' that defines the
	 *            desired path underneath the IRODS scratch directory
	 * @return <code>String</code> with trailing '/' that gives the absolute
	 *         path for an IRODS collection
	 * @throws TestingUtilsException
	 * @throws URISyntaxException
	 */
	public String buildIRODSCollectionAbsolutePathFromFederatedZoneWriteTestProperties(
			final Properties testingProperties,
			final String collectionPathBelowScratch)
			throws TestingUtilsException {

		if (testingProperties.get(IRODS_SCRATCH_DIR_KEY) == null) {
			throw new TestingUtilsException(
					"scratch path not provided in testing.properties");
		}

		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append('/');
		pathBuilder.append(testingProperties.get(IRODS_FEDERATED_ZONE_KEY));
		pathBuilder.append("/home/");
		pathBuilder.append(testingProperties.get(IRODS_FEDERATED_USER_KEY));
		pathBuilder.append("/fedwrite/");
		pathBuilder.append(testingProperties.get(IRODS_SCRATCH_DIR_KEY));
		pathBuilder.append('/');
		pathBuilder.append(collectionPathBelowScratch);
		return pathBuilder.toString();
	}

}
