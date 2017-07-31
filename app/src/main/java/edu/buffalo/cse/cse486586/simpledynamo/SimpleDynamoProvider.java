package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleDynamoProvider extends ContentProvider {
	// The port number of server
	static final int SERVER_PORT = 10000;

	// Id of each AVD
	static final String PORT0 = "5554";
	static final String PORT1 = "5556";
	static final String PORT2 = "5558";
	static final String PORT3 = "5560";
	static final String PORT4 = "5562";

	// Local variables
	private MyDBHelper dbHelper;
	private String myPort;
	private Uri mURI;
	private String portstr;
	private TreeSet<DynamoNode> allNodes;
	private ServerSocket server;
	private String[] ports;
	private String queryResult;
	private String buffMsg;

	private String failureNode;
	private KeyBuffer buffer;
	
	// Type of request
	static final String JOIN = "Join";
	static final String INSERT = "Insert";
	static final String DELETE = "Delete";
	static final String QUERY = "Query";
	static final String CONFIRM_INSERT = "InsertSuccess";
	static final String CONFIRM_DELETE = "DeleteSuccess";

	@Override
	public synchronized boolean onCreate() {
		// Create Db
		this.dbHelper = new MyDBHelper(getContext(), "kv.db", null, 1);
		// Build URI
		this.mURI = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		// Sort by their hash code(increasing order of String)
		this.allNodes = new TreeSet<DynamoNode>(new Comparator<DynamoNode>() {
			@Override
			public int compare(DynamoNode lhs, DynamoNode rhs) {
				return lhs.getHashCode().compareTo(rhs.getHashCode());
			}
		});

		// Generate port
		// ACK: Refer the code of previous assignments
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portstr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portstr) * 2);

		Log.e("My Code - Client", "My Port number : " + myPort);
		this.ports = new String[5];
		this.ports[0] = PORT0;
		this.ports[1] = PORT1;
		this.ports[2] = PORT2;
		this.ports[3] = PORT3;
		this.ports[4] = PORT4;

		this.queryResult = "";
		this.buffMsg = "";

		try {
			for (int i = 0;i < ports.length;i++) {
				String hash = genHash(ports[i]);
				DynamoNode node = new DynamoNode(ports[i], hash);
				allNodes.add(node);
			}
			Log.d("ONCREATE", "Alive nodes number:" + allNodes.size());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		try {
			this.server = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server);

		} catch(UnknownHostException e) {
			Log.e("My Code - Client", "Cannot create socket");
			return false;

		} catch(IOException e) {
			Log.e("My Code - Client", "Cannot create server socket");
			return false;
		}

		this.buffer = new KeyBuffer();
		this.failureNode = "";
		try {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, JOIN + "\n").get();
		} catch (Exception e) {
			e.printStackTrace();
		}

		String[] pairs = buffMsg.split("%");
		SQLiteDatabase dbWriter = dbHelper.getWritableDatabase();
		// Insert all missed pairs to recovered AVD
		ContentValues cv = new ContentValues();
		for (String missed : pairs) {
			if (missed.length() > 0) {
				String[] pair = missed.split(",");
				String key = pair[0];
				String value = pair[1];
				cv.put("key", key);
				cv.put("value", value);
				dbWriter.insertWithOnConflict("kv", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
			}
		}

		this.buffMsg = "";
		return true;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		private ReadWriteLock lock = new ReentrantReadWriteLock();

		@Override
		protected Void doInBackground(String... msgs) {
			String msg = msgs[0].trim();
			String[] tokens = msg.split(",");
			String type = tokens[0];

			if (JOIN.equals(type)) {
				for (DynamoNode n : allNodes) {
					if (portstr.equals(n.getEmuPort())) continue;
					try {
						lock.writeLock().lock();
						Socket client = new Socket();
						InetAddress address = InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
						int remotePort = Integer.parseInt(n.getEmuPort()) * 2;
						client.connect(new InetSocketAddress(address, remotePort));

						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
						String message = JOIN + "," + portstr + "\n";
						writer.write(message);
						Log.d("JOIN", "WRITE" + message + "to node :" + n.getEmuPort() + "from" + portstr);
						writer.flush();

						BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
						String bufferedPairs = reader.readLine().trim();

						Log.e("JOIN", "READ Buffer:" + bufferedPairs.length());

						buffMsg += bufferedPairs;
						client.close();
					} catch (UnknownHostException e) {
						Log.e("JOIN", "unknownHost");
					} catch (IOException e) {
						Log.e("JOIN", "IOException, cannot connect to target");
					} catch (NullPointerException e) {
						Log.e("JOIN", "cannot read from remote AVD");
					}
 					finally {
						lock.writeLock().unlock();
					}
				}
			}

			else if (INSERT.equals(type)) {
				// Forward insert msg to target AVD
				String key = tokens[1];
				String value = tokens[2];
				String target = tokens[3];
				try {
					lock.writeLock().lock();

					Socket client = new Socket();
					InetAddress address = InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
					int remotePort = Integer.parseInt(target) * 2;
					client.connect(new InetSocketAddress(address, remotePort));
					Log.e("Insert", "cannot connect!");
					client.setSoTimeout(1200);

					BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
					String message = INSERT + "," + key + "," + value + "\n";
					writer.write(message);
					writer.flush();

					BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
					Log.e("INSERT", "reader is null:" + (reader == null));
					String confirm = reader.readLine().trim();
					Log.e("INSERT", "Confirm:" + confirm);
					client.close();

				} catch (EOFException e) {
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Cannot connect to:" + target);
					Log.e("INSERT", "FAIL node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				} catch (SocketException e) {
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Cannot connect to:" + target);
					Log.e("INSERT", "FAIL node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				} catch (SocketTimeoutException e) {
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Socket Timeout");
					Log.e("INSERT", "FAIL node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				} catch (UnknownHostException e) {
					Log.e("INSERT", "unknown Host : 10.0.2.2");
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Cannot connect to:" + target);
					Log.e("INSERT", "FAIL node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				} catch (IOException e) {
					Log.e("INSERT", "Cannot connect to:" + target);
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Cannot connect to:" + target);
					Log.e("INSERT", "FALI node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				} catch (IllegalArgumentException e) {
					Log.e("INSERT", "Cannot connect to:" + target);
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Cannot connect to:" + target);
					Log.e("INSERT", "FALI node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				} catch (NullPointerException e) {
					Log.e("INSERT", "Cannot connect to:" + target);
					buffer.addPair(tokens[1], tokens[2]);
					handleFailure(target);
					Log.e("INSERT", "Cannot connect to:" + target);
					Log.e("INSERT", "FALI node : " + failureNode);
					Log.e("INSERT", "Buffered msg :" + tokens[1] + "||" + tokens[2]);
				}
				finally {
					lock.writeLock().unlock();
				}
			}

			else if (QUERY.equals(type)) {
				lock.readLock().lock();
				String field = tokens[1].trim();
				if ("*".equals(field)) {
					for (DynamoNode n : allNodes) {
						if (n.getEmuPort().equals(portstr) || n.getEmuPort().equals(failureNode))
							continue;
						queryResult += queryAll(n.getEmuPort());
					}
				} else {
					TreeSet<DynamoNode> targets = findTargetAVD(field);
					DynamoNode last = targets.last();
					DynamoNode first = targets.first();
					DynamoNode second = null;
					int i = 1;
					for (DynamoNode n : targets) {
						if (i == 2) {
							second = n;
							break;
						} else {
							i += 1;
						}
					}
					Log.d("QUERY", "Last:" + last.getEmuPort());
					Log.d("QUERY", "Second:" + second.getEmuPort());

					if (last.getEmuPort().equals(failureNode)) {
						queryResult += queryFrom(second.getEmuPort(), field);
						Log.d("QUERY", "queryResult1:" + queryResult);
					} else {
						queryResult += queryFrom(last.getEmuPort(), field);
						Log.d("QUERY", "queryResult3:" + queryResult.trim().equals(""));
						queryResult = queryResult.trim();

						if ("".equals(queryResult)) {
							queryResult += queryFrom(second.getEmuPort(), field);
						} else {
							String lastResult = queryFrom(last.getEmuPort(), field);
							String secondResult = queryFrom(second.getEmuPort(), field);
							if (secondResult.equals(lastResult)) {
								queryResult += lastResult;
							} else {
								queryResult += queryFrom(first.getEmuPort(), field);
							}
						}
						Log.d("QUERY", "queryResult2:" + queryResult);
					}
				}
				lock.readLock().unlock();
			}

			else if (DELETE.equals(type)) {
				if ("*".equals(tokens[1])) {
					for (DynamoNode node : allNodes) {
						if (node.getEmuPort().equals(failureNode)) {
						} else {
							try {
								Socket client = new Socket();
								InetAddress address = InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
								client.connect(new InetSocketAddress(address, Integer.parseInt(node.getEmuPort()) * 2));
								client.setSoTimeout(1200);
								// Send delete request to all alive nodes
								BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
								String message = DELETE + ",*\n";
								writer.write(message);
								writer.flush();

								BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
								String confirm = reader.readLine().trim();
								client.close();
							} catch (EOFException e) {
								handleFailure(node.getEmuPort());
								Log.e("DELETE", "Cannot connect to:" + node.getEmuPort());
							} catch (SocketException e) {
								handleFailure(node.getEmuPort());
								Log.e("DELETE", "Cannot connect to:" + node.getEmuPort());
							} catch (SocketTimeoutException e) {
								handleFailure(node.getEmuPort());
								Log.e("DELETE", "Socket Timeout");
							} catch (UnknownHostException e) {
								handleFailure(node.getEmuPort());
								Log.e("DELETE", "unknown Host : 10.0.2.2");
							} catch (IOException e) {
								handleFailure(node.getEmuPort());
								Log.e("DELETE", "Cannot connect to:" + node.getEmuPort());
							} catch (NullPointerException e) {
								handleFailure(node.getEmuPort());
								Log.e("DELETE", "Cannot connect to:" + node.getEmuPort());
							}
						}
					}
				} else {
					String key = tokens[1];
					String target = tokens[2];
					Socket client = new Socket();
					try {
						InetAddress address = InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
						client.connect(new InetSocketAddress(address, Integer.parseInt(target) * 2));
						client.setSoTimeout(1200);
						// Send delete request to other node
						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
						String message = DELETE + "," + key + "\n";
						writer.write(message);
						writer.flush();

						BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
						String confirm = reader.readLine().trim();
						client.close();
					} catch (EOFException e) {
						handleFailure(target);
						Log.e("DELETE", "Cannot connect to:" + target);
					} catch (SocketException e) {
						handleFailure(target);
						Log.e("DELETE", "Cannot connect to:" + target);
					} catch (SocketTimeoutException e) {
						handleFailure(target);
						Log.e("DELETE", "Socket Timeout");
					} catch (UnknownHostException e) {
						handleFailure(target);
						Log.e("DELETE", "unknown Host : 10.0.2.2");
					} catch (IOException e) {
						handleFailure(target);
						Log.e("DELETE", "Cannot connect to:" + target);
					} catch (NullPointerException e) {
						handleFailure(target);
						Log.e("DELETE", "Cannot connect to:" + target);
					}
				}
			}
			return null;
		}

		private synchronized String queryAll(String id) {
			BufferedWriter bw = null;
			Socket clientSocket = new Socket();
			MatrixCursor matrixCursor = null;

			try {
				clientSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id) * 2));
				clientSocket.setSoTimeout(1200);
				OutputStream os = clientSocket.getOutputStream();
				bw = new BufferedWriter(new OutputStreamWriter(os));

				String message = QUERY + ",*\n";
				bw.write(message);
				bw.flush();

				InputStream is = clientSocket.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String result = br.readLine();
				result = result.trim();

				if (br != null) {
					br.close();
				}

				clientSocket.close();

				String[] columns = new String[] {"key", "value"};
				matrixCursor = new MatrixCursor(columns);
				matrixCursor = buildCursor(matrixCursor, result);

			} catch (UnknownHostException e) {
				Log.e("Query", "ClientTask UnknownHostException");
			} catch (NumberFormatException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (NullPointerException e) {
				handleFailure(id);
				Log.e("Query", "ClientTask NullPointerException");
			}catch (SocketTimeoutException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (SocketException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (EOFException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

			return buildString(matrixCursor);
		}

		private synchronized String queryFrom(String id, String selection) {
			BufferedWriter bw = null;
			Socket clientSocket = new Socket();
			MatrixCursor matrixCursor = null;

			try {
				Log.d("QUERY FROM", "target port:" + id);

				clientSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id) * 2));
				clientSocket.setSoTimeout(1200);
				OutputStream os = clientSocket.getOutputStream();
				bw = new BufferedWriter(new OutputStreamWriter(os));

				String message = QUERY + "," + selection + "\n";
				bw.write(message);
				Log.d("QUERY FROM", "send msg:" + message);
				bw.flush();

				InputStream is = clientSocket.getInputStream();
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String result = br.readLine().trim();
				if (br != null) {
					br.close();
				}
				clientSocket.close();
				Log.d("QUERYFROM", "from result:" + result);
				String[] columns = new String[] { "key", "value" };
				matrixCursor = new MatrixCursor(columns);
				matrixCursor = buildCursor(matrixCursor, result);

			} catch (NullPointerException e) {
				handleFailure(id);
				Log.e("QUERY", "Null pointer, node failure");
			} catch (UnknownHostException e) {
				Log.e("Query", "ClientTask UnknownHostException");
			} catch (NumberFormatException e) {
				handleFailure(id);
				e.printStackTrace();
			}  catch (SocketTimeoutException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (SocketException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (EOFException e) {
				handleFailure(id);
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return buildString(matrixCursor);
		}
	}


	@Override
	public synchronized Uri insert(Uri uri, ContentValues values)  {
		SQLiteDatabase dbWriter = dbHelper.getWritableDatabase();
		String key = values.getAsString("key");
		String value = values.getAsString("value");

		try {
			TreeSet<DynamoNode> insertTargets = findTargetAVD(key);
			Log.d("INSERT", "key=" + key);

			for (DynamoNode n : insertTargets) {
				Log.d("INSERT", "target:" + n.getEmuPort());

				if (this.failureNode.equals(n.getEmuPort())) {
					this.buffer.addPair(key, value);
				} else if (this.portstr.equals(n.getEmuPort())) {
					dbWriter.insertWithOnConflict("kv", null, values, SQLiteDatabase.CONFLICT_REPLACE);
				} else {
					String msg = INSERT + ",";
					msg += key + ",";
					msg += value + ",";
					msg += n.getEmuPort() + "\n";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
				}
			}
			Log.d("AFTERINSERT", "FailureNode:" + failureNode);
		} catch (Exception e) {
			Log.e("Query", "SQL write failed due to " + e);
			e.printStackTrace();
		}

		return uri;
	}

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		SQLiteDatabase DBWriter = this.dbHelper.getWritableDatabase();
		if (selection == null || selection.length() == 0)  return 0;
		// If selection is "@", delete all records on localhost
		// If selection is "*", delete all records on all nodes
		if (selection.equals("@")) {
			int rowNum = DBWriter.delete("kv", null, null);
			return rowNum;
		} else if (selection.equals("*")) {
			int rowNum = DBWriter.delete("kv", null, null);
			// Let all other nodes delete their records
			String msg = "";
			msg += DELETE + "," + "*" + "\n";
			try {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return rowNum;
		} else {
			int rowNum = DBWriter.delete("kv", "key = ?", new String[]{selection});
			// If this key-value pair is stored on local AVD, we can return here directly
			if (rowNum > 0) {
				Set<DynamoNode> targets = findTargetAVD(selection);
				for (DynamoNode n : targets) {
					if (n.getEmuPort().equals(portstr)) continue;
					if (n.getEmuPort().equals(failureNode)) {
						this.buffer.removePair(selection);
					} else {
						String msg = DELETE + ",";
						msg += selection + ",";
						msg += n.getEmuPort() + "\n";
						try {
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				return rowNum;
			} else {
				Set<DynamoNode> targets = findTargetAVD(selection);
				for (DynamoNode n : targets) {
					if (n.getEmuPort().equals(failureNode)) {
						//this.buffer.removePair(selection);
					} else {
						String msg = DELETE + ",";
						msg += selection + ",";
						msg += n.getEmuPort() + "\n";
						try {
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				return rowNum;
			}
		}
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		SQLiteDatabase DBReader = this.dbHelper.getReadableDatabase();
		String[] cols = {"key", "value"};
		String[] rows = new String[2];
		MatrixCursor mc = new MatrixCursor(cols);

		// Return all records at locallost
		if ("@".equals(selection)) {
			Cursor cursor = DBReader.query("kv", null, null, null, null, null, null);
			if (cursor != null) {
				while (cursor.moveToNext()) {
					String key = cursor.getString(cursor.getColumnIndex("key"));
					String value = cursor.getString(cursor.getColumnIndex("value"));
					rows[0] = key;
					rows[1] = value;
					mc.addRow(rows);
				}
			}
			if (cursor != null) {
				cursor.close();
			}
			return mc;
		} else if ("*".equals(selection)) {
			Cursor cursor = DBReader.query("kv", null, null, null, null, null, null);
			if (cursor != null) {
				while (cursor.moveToNext()) {
					String key = cursor.getString(cursor.getColumnIndex("key"));
					String value = cursor.getString(cursor.getColumnIndex("value"));
					rows[0] = key;
					rows[1] = value;
					mc.addRow(rows);
				}
			}
			if (cursor != null) {
				cursor.close();
			}

			String message = QUERY + ",*\n";
			try {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		} else {
			TreeSet<DynamoNode> targets = findTargetAVD(selection);
			DynamoNode last = targets.last();
			if (portstr.equals(last.getEmuPort())) {
				Log.d("QUERY", "key is on this AVD");
				Cursor cursor = DBReader.query("kv", null, "key=?", new String[]{selection}, null, null, null);
				Log.d("QUERY", "Run query");
				if (cursor != null) {
					while (cursor.moveToNext()) {
						String key = cursor.getString(cursor.getColumnIndex("key"));
						String value = cursor.getString(cursor.getColumnIndex("value"));
						rows[0] = key;
						rows[1] = value;
						mc.addRow(rows);
					}
				}
				if (cursor != null) {
					cursor.close();
				}
			} else {
				String message = QUERY + "," + selection + "\n";
				Log.d("QUERY", "Query request:" + selection);
				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		Log.e("QUERY", "Current query result:" + queryResult);
		mc = buildCursor(mc, queryResult);
		queryResult = "";
		return mc;
	}


	private void handleFailure(String nodeID) {
		failureNode = nodeID;
		buffer.setNodeId(nodeID);
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket clientSocket = null;

			while (true) {
				try {
					ReadWriteLock lock = new ReentrantReadWriteLock();
					//Log.e("My Code - Server", "Try block in Server thread");
					clientSocket = serverSocket.accept();
					BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String msg = reader.readLine().trim();
					String[] tokens = msg.split(",");
					String type = tokens[0];

					SQLiteDatabase dbWriter = dbHelper.getWritableDatabase();
					SQLiteDatabase dbReader = dbHelper.getReadableDatabase();

					if (JOIN.equals(type)) {
						String sourcePort = tokens[1];
						Log.e("JOIN Server", "source:" + sourcePort + "failure node:" + failureNode);
						Log.e("JOIN Server", "equal:" + failureNode.equals(sourcePort));
						if (failureNode.equals(sourcePort)) {
							failureNode = "";
						}

						int bufferedSize = buffer.getPairs().size();
						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

						if (buffer.getNodeId().equals(sourcePort) && bufferedSize > 0) {
							List<String> bufferedKV = buffer.getPairs();
							String response = "";
							for (String str : bufferedKV) {
								response += (str + "%");
							}

							response += "\n";
							writer.write(response);
							writer.flush();

							buffer.clean();
							failureNode = "";
						} else {
							writer.write("\n");
							writer.flush();
						}

						clientSocket.close();
					}

					else if (INSERT.equals(type)) {
						lock.writeLock().lock();
						// Get k,v and insert into local AVD
						String key = tokens[1];
						String value = tokens[2];
						ContentValues cv = new ContentValues();
						cv.put("key", key);
						cv.put("value", value);
						dbWriter.insertWithOnConflict("kv", null, cv, SQLiteDatabase.CONFLICT_REPLACE);

						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
						writer.write(CONFIRM_INSERT + "\n");
						writer.flush();

						lock.writeLock().unlock();
						clientSocket.close();
					}

					else if (QUERY.equals(type)) {
						lock.readLock().lock();
						String field = tokens[1];
						MatrixCursor mc = null;
						if ("*".equals(field)) {
							mc = (MatrixCursor) query(mURI, null, "@", null, null);
						} else {
							Log.d("RECEQUERYREQ", "target key:" + field);
							Cursor cursor = dbReader.query("kv", null, "key = ?", new String[]{field}, null, null, null);
							Log.d("RECEQUERY", "result cursor:" + cursor.getCount());
							mc = new MatrixCursor(new String[]{"key", "value"});
							String[] rows = new String[2];
							if (cursor != null) {
								while (cursor.moveToNext()) {
									String key = cursor.getString(cursor.getColumnIndex("key"));
									String value = cursor.getString(cursor.getColumnIndex("value"));
									rows[0] = key;
									rows[1] = value;
									mc.addRow(rows);
								}
							}
							if (cursor != null) {
								cursor.close();
							}
						}
						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
						writer.write(buildString(mc));
						Log.d("QUERYRETURN", "result string:" + buildString(mc));
						writer.flush();
						lock.readLock().unlock();
						clientSocket.close();
					}

					else if (DELETE.equals(type)) {
						lock.writeLock().lock();
						String selection = tokens[1];
						if ("*".equals(selection)) {
							Log.d("Delete", "selection:" + selection);
							delete(mURI, "@", null);
						} else {
							Log.d("Delete", "selection:" + selection);
							int row = dbWriter.delete("kv", "key=?", new String[]{selection});
						}
						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
						writer.write(CONFIRM_DELETE + "\n");
						writer.flush();
						lock.writeLock().unlock();
						clientSocket.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					break;
				} catch (NullPointerException e) {
					e.printStackTrace();
				}
			}
			return null;
		}
	}

	// Find the inserting target AVDs for this key
	private TreeSet<DynamoNode> findTargetAVD(String key) {
		HashMap<String, Integer> portsMap= new HashMap<String, Integer>();
		String hashCode = "";
		try {
			hashCode = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.e("INSERT", "cannot generate hashCode for this key!");
		}

		TreeSet<DynamoNode> result = new TreeSet<DynamoNode>(new Comparator<DynamoNode>() {
			@Override
			public int compare(DynamoNode lhs, DynamoNode rhs) {
				return lhs.getHashCode().compareTo(rhs.getHashCode());
			}
		});
		List<DynamoNode> nodesList = new ArrayList<DynamoNode>();

		int i = 0;
		for (DynamoNode n : allNodes) {
			nodesList.add(n);
			portsMap.put(n.getEmuPort(), i++);
		}

		int N = 3;
		int coordinatorIdx = -1;
		for (DynamoNode n : allNodes) {
			if (hashCode.compareTo(n.getHashCode()) < 0) {
				coordinatorIdx = portsMap.get(n.getEmuPort());
				break;
			}
		}

		if (coordinatorIdx == -1) {
			for (int j = 0;j < N;j++) {
				result.add(nodesList.get(j));
			}
		} else {
			result.add(nodesList.get(coordinatorIdx));
			result.add(nodesList.get((coordinatorIdx + 1) % 5));
			result.add(nodesList.get((coordinatorIdx + 2) % 5));
		}

		return result;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		return 0;
	}


	// =========================Util Functions===========================
	// URI build Function
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	// Build matrixCursor from query result string
	private MatrixCursor buildCursor(MatrixCursor mc, String str) {
		if (str == null || str.length() == 0) {
			return mc;
		}

		if (str.length() != 0) {
			String[] ss = str.split(",");
			for (String s : ss) {
				if (s.length() > 0) {
					String[] temp = s.split("=");
					if (temp.length > 1) {
						String[] kv = new String[2];
						kv[0] = temp[0].trim();
						kv[1] = temp[1].trim();
						mc.addRow(kv);
					}
				}
			}
		}
		return mc;
	}

	// Build query response string from Cursor
	private String buildString(MatrixCursor mc) {
		if (mc == null || mc.getCount() == 0) {
			return "\n";
		}

		String result = "";
		mc.moveToFirst();
		while (!mc.isAfterLast()) {
			result += mc.getString(0);
			result += "=";
			result += mc.getString(1);
			result += ",";
			mc.moveToNext();
		}
		result += "\n";
		return result;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}