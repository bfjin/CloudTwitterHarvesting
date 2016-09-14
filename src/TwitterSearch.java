import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mpi.*;

public class TwitterSearch {
	public static void main(String args[]) throws Exception {
		// Initialize MPI
		long startTime = System.currentTimeMillis();
		MPI.Init(args);
		int me = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		final int ROOT = 0;
		// The search term
		String term = "follow";
		int termCount = 0;
		

		// The hashmap for storing the relationship between the name and the
		// count
		Map<String, Integer> mentionedUsersMap = new HashMap<String, Integer>();
		Map<String, Integer> mentionedTopicsMap = new HashMap<String, Integer>();

		RandomAccessFile raf = null;
		try {
			// Opens the csv
			raf = new RandomAccessFile(new File("twitter.csv"), "r");
			// Set each of the process to a different location within the file
			// According to their rank
			raf.seek(me * (raf.length() / size));

			long off = 0;
			// If the starting line does not contain the "text" tag
			// Means we have probably miss the text content
			if (raf.readLine().indexOf("\"text\"") == -1) {
				raf.seek(me * (raf.length() / size));
				// We go to the next newline
				while (raf.read() != '\n') {
					off += 1;
				}
				off += 1;
				// and start from there
				raf.seek((me * (raf.length() / size)) + off);

			}

			long[] sendBuf = { off };
			long[] recvBuf = { off };
			// Now we send the new location to the process before the  
			// current one so they can update their new end position
			// Rank 0 does not need to send offset
			if (me != 0) {
				MPI.COMM_WORLD.Send(sendBuf, 0, sendBuf.length, MPI.LONG,
						me - 1, 0);
			}
			
			// and the last one does not need to receive
			if (me != size - 1) {
				MPI.COMM_WORLD.Recv(recvBuf, 0, recvBuf.length, MPI.LONG,
						me + 1, 0);
			}
			// Everyone now sets their ending position which is where the next
			// one starts
			long border = (me + 1) * (raf.length() / size) + recvBuf[0];
			// The last process can set his ending position to the end of the
			// file
			if (me == size - 1) {
				border = raf.length();
			}

			String availalbe;
			// When the end of the line is not reached and the current position
			// has not reached the end position
			while ((availalbe = raf.readLine()) != null
					&& raf.getFilePointer() < border) {
				String textContent = "";
				// Extracting the actual text content
				try {
					textContent = availalbe.substring(
							availalbe.indexOf("\"text\"") + 10,
							availalbe.indexOf("\"in_reply_to_status_id\"") - 4);
				} catch (IndexOutOfBoundsException e) {
					textContent = availalbe.substring(availalbe
							.indexOf("\"text\"") + 10);
				}

				// Search term pattern matching
				String termPattern = "\\b" + term.toLowerCase() + "\\b";
				Pattern termP = Pattern.compile(termPattern);
				Matcher termM = termP.matcher(textContent.toLowerCase());
				while (termM.find()) {
					termCount++;
				}

				// User pattern matching
				String userPattern = "@\\w+";
				Pattern userP = Pattern.compile(userPattern);
				Matcher userM = userP.matcher(textContent);
				while (userM.find()) {
					String userName = userM.group().substring(1);
					if (mentionedUsersMap.get(userName) == null) {
						mentionedUsersMap.put(userName, 1);
					} else {
						mentionedUsersMap.put(userName,
								mentionedUsersMap.get(userName) + 1);
					}

				}

				// Topic pattern matching
				String topicPattern = "#\\w+";
				Pattern topicP = Pattern.compile(topicPattern);
				Matcher topicM = topicP.matcher(textContent);
				while (topicM.find()) {
					String topicName = topicM.group().substring(1);
					if (mentionedTopicsMap.get(topicName) == null) {
						mentionedTopicsMap.put(topicName, 1);
					} else {
						mentionedTopicsMap.put(topicName,
								mentionedTopicsMap.get(topicName) + 1);
					}

				}

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// Close the file
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Wait other process to complete their task
		MPI.COMM_WORLD.Barrier();

		// Now all worker nodes sends their result back to the root node
		// First term count
		if (me != ROOT) {
			int[] sendBuf = { termCount };
			MPI.COMM_WORLD.Send(sendBuf, 0, sendBuf.length, MPI.INT, ROOT, 0);
		} else {
			int[] recvBuf = { 0 };
			for (int i = 1; i < size; i++) {
				MPI.COMM_WORLD.Recv(recvBuf, 0, recvBuf.length, MPI.INT, i, 0);
				// Root node adds the result from the worker
				termCount += recvBuf[0];
			}
		}

		// Then the users count
		if (me != ROOT) {
			byte[] sendBuf = convertToBytes(mentionedUsersMap);
			int[] sendLength = { sendBuf.length };
			MPI.COMM_WORLD.Send(sendLength, 0, sendLength.length, MPI.INT,
					ROOT, 0);

			MPI.COMM_WORLD.Send(sendBuf, 0, sendBuf.length, MPI.BYTE, ROOT, 0);
		} else {
			int[] recvLength = { 0 };
			byte[] recvBuf = null;
			for (int i = 1; i < size; i++) {
				MPI.COMM_WORLD.Recv(recvLength, 0, recvLength.length, MPI.INT,
						i, 0);
				recvBuf = new byte[recvLength[0]];
				MPI.COMM_WORLD.Recv(recvBuf, 0, recvBuf.length, MPI.BYTE, i, 0);
				@SuppressWarnings("unchecked")
				Map<String, Integer> mentionedUsersMapFromWorker =
						(Map<String, Integer>) convertFromBytes(recvBuf);
				List<String> keys = new ArrayList<String>(
						mentionedUsersMapFromWorker.keySet());
				// Root node adds the result from the worker
				for (String key : keys) {
					if (mentionedUsersMap.get(key) == null) {
						mentionedUsersMap.put(key,
								mentionedUsersMapFromWorker.get(key));
					} else {
						mentionedUsersMap.put(key, mentionedUsersMap.get(key)
								+ mentionedUsersMapFromWorker.get(key));
					}
				}
			}
		}

		// Then the topics count
		if (me != ROOT) {
			byte[] sendBuf = convertToBytes(mentionedTopicsMap);
			int[] sendLength = { sendBuf.length };
			MPI.COMM_WORLD.Send(sendLength, 0, sendLength.length, MPI.INT,
					ROOT, 0);

			MPI.COMM_WORLD.Send(sendBuf, 0, sendBuf.length, MPI.BYTE, ROOT, 0);
		} else {
			int[] recvLength = { 0 };
			byte[] recvBuf = null;
			for (int i = 1; i < size; i++) {
				MPI.COMM_WORLD.Recv(recvLength, 0, recvLength.length, MPI.INT,
						i, 0);
				recvBuf = new byte[recvLength[0]];
				MPI.COMM_WORLD.Recv(recvBuf, 0, recvBuf.length, MPI.BYTE, i, 0);
				@SuppressWarnings("unchecked")
				Map<String, Integer> mentionedTopicsMapFromWorker = 
						(Map<String, Integer>) convertFromBytes(recvBuf);
				List<String> keys = new ArrayList<String>(
						mentionedTopicsMapFromWorker.keySet());
				// Root node adds the result from the worker
				for (String key : keys) {
					if (mentionedTopicsMap.get(key) == null) {
						mentionedTopicsMap.put(key,
								mentionedTopicsMapFromWorker.get(key));
					} else {
						mentionedTopicsMap.put(key, mentionedTopicsMap.get(key)
								+ mentionedTopicsMapFromWorker.get(key));
					}
				}
			}
		}
		// End of MPI
		MPI.Finalize();
		
		// Root node prints all the result
		if (me == 0) {
			List<String> topTenUsers = findTopTen(mentionedUsersMap);
			List<String> topTenTopics = findTopTen(mentionedTopicsMap);

			System.out.println("Search term = "
					+ term + ", " + "term count = " + termCount);
			for (String user : topTenUsers) {
				System.out.println("User = " + user + ", " + "count = "
						+ mentionedUsersMap.get(user));
			}
			for (String topic : topTenTopics) {
				System.out.println("Topic = " + topic + ", " + "count = "
						+ mentionedTopicsMap.get(topic));
			}
			
			// Calculate time
			long endTime = System.currentTimeMillis();
			long elapsedTime = startTime - endTime;
			System.out.println("Elapsed time: " + formatInterval(elapsedTime));
		}
	}

	// Serialize an object to a byte array
	private static byte[] convertToBytes(Object object) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutput out = new ObjectOutputStream(bos)) {
			out.writeObject(object);
			return bos.toByteArray();
		}
	}

	// Convert byte array back to an object
	private static Object convertFromBytes(byte[] bytes) throws IOException,
			ClassNotFoundException {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
				ObjectInput in = new ObjectInputStream(bis)) {
			return in.readObject();
		}
	}

	// Find top ten elements given a map
	private static List<String> findTopTen(Map<String, Integer> map) {
		List<String> topTen = new ArrayList<String>(10);
		List<String> mapKeys = new ArrayList<String>(map.keySet());
		List<Integer> mapValues = new ArrayList<Integer>(map.values());
		Collections.sort(mapValues);
		mapValues = mapValues.subList(mapValues.size() - 10, mapValues.size());
		Collections.reverse(mapValues);
		for (Integer values : mapValues) {
			for (String key : mapKeys) {
				if (map.get(key) == values) {
					if (!topTen.contains(key)) {
						topTen.add(key);
						break;
					}
				}
			}
		}
		return topTen;
	}
	
	// Time format to hh:mm:ss:mss
    private static String formatInterval(final long l)
    {
        final long hr = TimeUnit.MILLISECONDS.toHours(l);
        final long min = TimeUnit.MILLISECONDS.toMinutes(l - TimeUnit.HOURS.toMillis(hr));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
        final long ms = TimeUnit.MILLISECONDS.toMillis(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
        return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms);
    }

}
