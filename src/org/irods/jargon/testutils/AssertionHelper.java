/**
 *
 */
package org.irods.jargon.testutils;

import static org.irods.jargon.testutils.TestingPropertiesHelper.GENERATED_FILE_DIRECTORY_KEY;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;

import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.testutils.filemanip.ScratchFileUtils;

/**
 * Helpful assertions for unit testing IRODS
 * 
 * @author Mike Conway, DICE (www.irods.org)
 * @since
 * 
 */
public class AssertionHelper {
	private Properties testingProperties = new Properties();
	private TestingPropertiesHelper testingPropertiesHelper = new TestingPropertiesHelper();
	private ScratchFileUtils scratchFileUtils = null;
	private static final String ASSERTION_ERROR_MESSAGE = "assertion failed -- ";
	private static final String FILE_DOES_NOT_EXIST_ERROR = "requested file does not exist!";

	public AssertionHelper() throws TestingUtilsException {
		testingProperties = testingPropertiesHelper.getTestProperties();
		scratchFileUtils = new ScratchFileUtils(testingProperties);
	}

	/**
	 * Ensures that a scratch file does not exist given the path/file name
	 * 
	 * @param filePathRelativeToScratch
	 *            <code>String</code> that gives the relative file path under
	 *            scratch, with no leading separator character
	 * @throws IRODSTestAssertionException
	 */
	public void assertLocalFileNotExistsInScratch(
			final String filePathRelativeToScratch)
			throws IRODSTestAssertionException {
		StringBuilder fullPathToLocalFile = computeFullPathToLocalFile(filePathRelativeToScratch);
		StringBuilder errorMessage = new StringBuilder();
		errorMessage.append(ASSERTION_ERROR_MESSAGE);
		errorMessage.append("local file exists and should not");
		errorMessage.append(fullPathToLocalFile);
		File localFile = new File(fullPathToLocalFile.toString());
		if (localFile.exists()) {
			throw new IRODSTestAssertionException(errorMessage.toString());
		}

	}

	/**
	 * Ensures that a file exists given the path/file name
	 * 
	 * @param filePathRelativeToScratch
	 *            <code>String</code> that gives the relative file path under
	 *            scratch, with no leading separator character
	 * @throws IRODSTestAssertionException
	 */
	public void assertLocalFileExistsInScratch(
			final String filePathRelativeToScratch)
			throws IRODSTestAssertionException {
		StringBuilder fullPathToLocalFile = computeFullPathToLocalFile(filePathRelativeToScratch);
		StringBuilder errorMessage = new StringBuilder();
		errorMessage.append(ASSERTION_ERROR_MESSAGE);
		errorMessage.append("local file does not exist:");
		errorMessage.append(fullPathToLocalFile);
		File localFile = new File(fullPathToLocalFile.toString());
		if (!localFile.exists()) {
			throw new IRODSTestAssertionException(errorMessage.toString());
		}

	}

	/**
	 * Ensures that the given file has the expected length
	 * 
	 * @param filePathRelativeToScratch
	 *            <code>String</code> that gives the relative file path under
	 *            scratch, with no leading separator character
	 * @param expectedLength
	 *            <code>long</code> with length in KB of file that is expected
	 * @throws IRODSTestAssertionException
	 */
	public void assertLocalScratchFileLengthEquals(
			final String filePathRelativeToScratch, final long expectedLength)
			throws IRODSTestAssertionException {
		StringBuilder fullPathToLocalFile = computeFullPathToLocalFile(filePathRelativeToScratch);
		File localFile = new File(fullPathToLocalFile.toString());
		if (!localFile.exists()) {
			throw new IRODSTestAssertionException(FILE_DOES_NOT_EXIST_ERROR);
		}
		if (localFile.length() != expectedLength) {
			StringBuilder errorMessage = new StringBuilder();
			errorMessage.append(ASSERTION_ERROR_MESSAGE);
			errorMessage.append("file length error, expected:");
			errorMessage.append(expectedLength);
			errorMessage.append(" actual:");
			errorMessage.append(localFile.length());
			errorMessage.append(" for file:");
			errorMessage.append(fullPathToLocalFile);
			throw new IRODSTestAssertionException(errorMessage.toString());
		}
	}

	/**
	 * Ensure that the given local file exists and has the expected checksum
	 * value
	 * 
	 * @param filePathRelativeToScratch
	 *            <code>String</code> that gives the relative file path under
	 *            scratch, with no leading separator character
	 * @param actualChecksum2
	 *            <code>long</code> value with the anticipated MD5 checksum
	 * @throws IRODSTestAssertionException
	 */
	public void assertLocalFileHasChecksum(
			final String filePathRelativeToScratch,
			final byte[] expectedChecksum) throws IRODSTestAssertionException {
		byte[] actualChecksum;

		try {
			actualChecksum = scratchFileUtils
					.computeFileCheckSum(filePathRelativeToScratch);
			boolean areEqual = Arrays.equals(actualChecksum, expectedChecksum);
			if (!areEqual) {
				StringBuilder errorMessage = new StringBuilder();
				errorMessage.append(ASSERTION_ERROR_MESSAGE);
				errorMessage.append("checksum error, expected:");
				errorMessage.append(String.valueOf(expectedChecksum));
				errorMessage.append(" actual:");
				errorMessage.append(String.valueOf(actualChecksum));
				errorMessage.append(" for file:");
				errorMessage.append(filePathRelativeToScratch);
				throw new IRODSTestAssertionException(errorMessage.toString());

			}
		} catch (TestingUtilsException e) {
			StringBuilder message = new StringBuilder();
			message.append("error when computing checksum on file:");
			message.append(filePathRelativeToScratch);
			throw new IRODSTestAssertionException(message.toString(), e);
		}
	}

	protected StringBuilder computeFullPathToLocalFile(
			final String filePathRelativeToScratch) {
		StringBuilder fullPathToLocalFile = new StringBuilder();
		fullPathToLocalFile.append(testingProperties
				.get(GENERATED_FILE_DIRECTORY_KEY));
		fullPathToLocalFile.append(filePathRelativeToScratch);
		return fullPathToLocalFile;
	}

	public void assertIrodsFileMatchesLocalFileChecksum(
			final String absoluteIRODSPathUnderScratch,
			final String absoluteLocalFileUnderScratch)
			throws IRODSTestAssertionException {

		// FIXME: need to update for jargon-core
	}

	/**
	 * Make sure that a file or collection is in IRODS
	 * 
	 * @param absoluteIrodsPathUnderScratch
	 *            <code>String</code> with absolute path (leading '/', or a path
	 *            and filename to look for
	 * @throws IRODSTestAssertionException
	 */
	public void assertIrodsFileOrCollectionExists(
			final String absoluteIrodsPathUnderScratch,
			final IRODSAccessObjectFactory irodsAccessObjectFactory,
			final IRODSAccount irodsAccount) throws IRODSTestAssertionException {

		try {
			IRODSFileFactory fileFactory = irodsAccessObjectFactory
					.getIRODSFileFactory(irodsAccount);
			IRODSFile file = fileFactory
					.instanceIRODSFile(absoluteIrodsPathUnderScratch);
			if (!file.exists()) {
				throw new IRODSTestAssertionException("file does not exist");
			}

		} catch (JargonException e) {
			throw new IRODSTestAssertionException(e.getMessage());
		}

	}

	/**
	 * Make sure that a file or collection is not in IRODS
	 * 
	 * @param relativeIrodsPathUnderScratch
	 *            <code>String</code> with absolute path
	 * 
	 * @throws IRODSTestAssertionException
	 * 
	 */
	public void assertIrodsFileOrCollectionDoesNotExist(
			final String absoluteIrodsPathUnderScratch,
			final IRODSAccessObjectFactory irodsAccessObjectFactory,
			final IRODSAccount irodsAccount) throws IRODSTestAssertionException {

		try {
			IRODSFileFactory fileFactory = irodsAccessObjectFactory
					.getIRODSFileFactory(irodsAccount);
			IRODSFile file = fileFactory
					.instanceIRODSFile(absoluteIrodsPathUnderScratch);
			if (file.exists()) {
				throw new IRODSTestAssertionException("file exists");
			}

		} catch (JargonException e) {
			throw new IRODSTestAssertionException(e.getMessage());
		}

	}

	/**
	 * Are two directory trees equal? Take two absolute paths to the local file
	 * system, recursively walk each tree and compare length, file name, and
	 * number of subdirectories/files.
	 * 
	 * @param dir1
	 *            <code>String<code> with the absolute path to a directory
	 * @param dir2
	 *            <code>String<code> with
	 * @throws IRODSTestAssertionException
	 */
	public void assertLocalDirectoriesHaveSameData(final String dir1,
			final String dir2) throws IRODSTestAssertionException {
		File file1 = new File(dir1);
		File file2 = new File(dir2);

		if (file1.exists() && file1.isDirectory()) {
			// ok
		} else {
			throw new IRODSTestAssertionException(
					"the first specified directory does not exist, or is not a directory");
		}

		if (file2.exists() && file2.isDirectory()) {
			// ok
		} else {
			throw new IRODSTestAssertionException(
					"the second specified directory does not exist, or is not a directory");
		}

		// side by side comparison

		File[] file1Files = file1.listFiles();
		File[] file2Files = file2.listFiles();

		if (file1Files.length != file2Files.length) {
			throw new IRODSTestAssertionException(
					"mismatch of number of files in a directory, file1 has:"
							+ file1Files.length + " files, while file2 has:"
							+ file2Files.length);
		}

		for (int i = 0; i < file1Files.length; i++) {
			assertTwoFilesAreEqualByRecursiveTreeComparison(file1Files[i],
					file2Files[i]);
		}

	}

	/**
	 * Recursively match two files/directories for length, number of members,
	 * and name
	 * 
	 * @param file1
	 *            <code>File</code> with a file or directory
	 * @param file2
	 *            <code>File<code> with a file or directory
	 * @throws IRODSTestAssertionException
	 */
	public void assertTwoFilesAreEqualByRecursiveTreeComparison(
			final File file1, final File file2)
			throws IRODSTestAssertionException {

		if (file1.getName().equals(".DS_Store")
				|| file2.getName().equals(".DS_Store")) {
			throw new IRODSTestAssertionException(
					"test data corrupted by Mac .DS_Store files, please reinitialize the scratch directories");
		}

		if (file1.exists() && file2.exists()) {
			// ok
		} else {
			throw new IRODSTestAssertionException(
					"compared files that do not exist in both trees - \nfile1:"
							+ file1.getAbsolutePath() + " \nfile2:"
							+ file2.getAbsolutePath());
		}

		if (file1.isDirectory() && file2.isDirectory()) {
			File[] file1Files = file1.listFiles();
			File[] file2Files = file2.listFiles();
			Arrays.sort(file1Files, new DirAlphaComparator());
			Arrays.sort(file2Files, new DirAlphaComparator());

			if (file1Files.length != file2Files.length) {
				throw new IRODSTestAssertionException(
						"directories differ in the number of files contained, dir1 is: "
								+ file1.getAbsolutePath() + "\n and has"
								+ +file1Files.length
								+ " children \n while dir2 is:"
								+ file2.getAbsolutePath() + " \n and has "
								+ file2Files.length + " children");
			}

			for (int i = 0; i < file1Files.length; i++) {
				assertTwoFilesAreEqualByRecursiveTreeComparison(file1Files[i],
						file2Files[i]);
			}

		} else if (file1.isFile() && file2.isFile()) {
			if (file1.length() != file2.length()) {
				throw new IRODSTestAssertionException(
						"file lengths differ,, file1 is: "
								+ file1.getAbsolutePath()
								+ "\n and has length:" + file1.length()
								+ "\n while file is:" + file2.getAbsolutePath()
								+ " \n and has length:" + file2.length());
			}

			if (file1.getName().equals(file2.getName())) {
				// names are equal
			} else {
				throw new IRODSTestAssertionException(
						"file names are different, file1 abs path is:"
								+ file1.getAbsolutePath()
								+ " while file2 abs path is:"
								+ file2.getAbsolutePath());
			}

		} else {
			throw new IRODSTestAssertionException(
					"file mismatch, one is a file, the other is a directory - file1:"
							+ file1.getAbsolutePath() + " file2:"
							+ file2.getAbsolutePath());
		}

	}

}

class DirAlphaComparator implements Comparator<File> {

	// Comparator interface requires defining compare method.
	@Override
	public int compare(final File filea, final File fileb) {
		// ... Sort directories before files,
		// otherwise alphabetical ignoring case.
		if (filea.isDirectory() && !fileb.isDirectory()) {
			return -1;

		} else if (!filea.isDirectory() && fileb.isDirectory()) {
			return 1;

		} else {
			return filea.getName().compareToIgnoreCase(fileb.getName());
		}
	}
}
