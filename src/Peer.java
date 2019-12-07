import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class Peer implements Runnable {
    private static final int PEER_ID = 1;
    private static ConcurrentHashMap<Integer, byte[]> summaryList = new ConcurrentHashMap<>();
    private static List<Integer> masterList = new ArrayList<>();
    private static String fileName;
    private int ownerPort, peerPort, downloadPort;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;

    public Peer(int ownerPort, int peerPort, int downloadPort) {
        this.ownerPort = ownerPort;
        this.peerPort = peerPort;
        this.downloadPort = downloadPort;
    }

    private void runPeer() {
        File chunksFolder = new File("temp");
        if (!chunksFolder.exists())
            chunksFolder.mkdir();
        downloadChunksFromFileOwner();
        connectToDownloadPeer();
        connectToUploadNeighbor();
    }

    private void connectToDownloadPeer() {
        (new Thread(this::downloadChunksFromPeer)).start();
    }


    private void connectToUploadNeighbor() {

        ServerSocket peerServerSocket = null;
        try {
            peerServerSocket = new ServerSocket(peerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            PeerHandler uploadNeighborSocket = new PeerHandler();
            try {
                Socket socket = peerServerSocket.accept();
                uploadNeighborSocket.initIOStreams(socket);
                uploadNeighborSocket.storeChunks(summaryList);
                uploadNeighborSocket.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean hasAllChunks() {
        for (int chunk : masterList) {
            if (!summaryList.containsKey(chunk)) {
                return false;
            }
        }
        combineChunks();
        return true;
    }

    private void combineChunks() {
        try {
            File file = new File(fileName);
            if (file.exists())
                file.delete();
            FileOutputStream fos = new FileOutputStream(file);
            for (int i = 0; i < masterList.size(); i++) {
                //saveChunk(i, summaryList.get(masterList.get(i)));
                fos.write(summaryList.get(masterList.get(i)));
            }
            fos.flush();
            fos.close();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Download complete for Peer " + PEER_ID + ".");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendMessage(ObjectOutputStream outputStream, String message) throws IOException {
        outputStream.writeObject(message);
        outputStream.flush();
        outputStream.reset();
    }

    private static void sendMessage(ObjectOutputStream oout,
                                    int intMessage) throws IOException {
        oout.writeInt(intMessage);
        oout.flush();
    }


    private void saveChunk(int chunkNumber, byte[] chunk) {
        try {
            FileOutputStream fos = new FileOutputStream("temp/" + chunkNumber, false);
            fos.write(chunk);
            fos.flush();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initIOStreams(Socket socket) throws IOException {
        outputStream = new ObjectOutputStream(socket.getOutputStream());
        inputStream = new ObjectInputStream(socket.getInputStream());
    }

    @Override
    public void run() {
        start();
    }

    public void start() {
        System.out.println("Peer started.");
        runPeer();
    }

    public static void main(String[] args) {
        if (args.length < 3)
            throw new IllegalArgumentException("Run Peer with valid listening ports");
        int owner = -1, peer = -1, download = -1;
        try {
            owner = Integer.parseInt(args[0]);
            peer = Integer.parseInt(args[1]);
            download = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Arguments passed must be valid port numbers");
            System.exit(1);
        }
        new Peer(owner, peer, download).start();
    }

    private Socket connectToServerWithRetry(int portNo) {
        int tryNo = 0;
        Socket socket;
        while (true) {
            try {
                socket = new Socket("localhost", portNo);
                if (socket.isConnected())
                    break;
            } catch (ConnectException e) {
                try {
                    tryNo++;
                    long timeToWait = (long) Math.min(2048, Math.pow(2, tryNo));
                    System.out.println("Retrying connection to Port: " + portNo);
                    Thread.sleep(timeToWait);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return socket;
    }

    private void downloadChunksFromFileOwner() {
        Socket peerSocket = connectToServerWithRetry(ownerPort);
        try {
            initIOStreams(peerSocket);
            sendMessage(outputStream, "FILE_NAME");
            fileName = (String) inputStream.readObject();

            System.out.println("Requesting chunk list from File Owner");
            sendMessage(outputStream, "CHUNK_LIST");
            sendMessage(outputStream, PEER_ID);
            masterList = (ArrayList<Integer>) inputStream.readObject();
            System.out.println("Received chunk list from File Owner");

            int requestedChunkNumber = (PEER_ID - 1) % 5;
            while (requestedChunkNumber < masterList.size()) {
                sendMessage(outputStream, "REQUEST_CHUNK");
                sendMessage(outputStream, requestedChunkNumber);
                sendMessage(outputStream, PEER_ID);
                System.out.println("Requested chunk " + requestedChunkNumber +
                        " from File Owner.");
                int receivedChunkNumber = inputStream.readInt();
                byte[] chunk = (byte[]) inputStream.readObject();
                summaryList.put(receivedChunkNumber, chunk);
                saveChunk(receivedChunkNumber, chunk);
                System.out.println("Received chunk " + receivedChunkNumber +
                        " from file owner.");
                requestedChunkNumber += 5;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void downloadChunksFromPeer() {
        Socket downloadSocket = connectToServerWithRetry(downloadPort);
        try {
            Thread.sleep(15000);
            initIOStreams(downloadSocket);
            while (!hasAllChunks()) {
                System.out.println("Requesting chunk list from download peer");
                sendMessage(outputStream, "CHUNK_LIST");
                List<Integer> availableChunksList = (ArrayList<Integer>) inputStream.readObject();
                TreeSet<Integer> peerSummarySet = new TreeSet<>(summaryList.keySet());
                System.out.println("Received chunk list from download peer");
                TreeSet<Integer> availableSet = new TreeSet<>(availableChunksList);
                if (availableSet.equals(peerSummarySet))
                    System.out.println("Peer has the same chunk list as download neighbor");

                availableSet.removeAll(peerSummarySet);
                if (!availableSet.isEmpty()) {
                    int chunkNumber = availableSet.first();
                    sendMessage(outputStream, "REQUEST_CHUNK");
                    sendMessage(outputStream, chunkNumber);
                    System.out.println("Request chunk " + chunkNumber + " from download neighbor");
                    int position = inputStream.readInt();
                    byte[] chunk = (byte[]) inputStream.readObject();
                    summaryList.put(position, chunk);
                    saveChunk(position, chunk);
                    System.out.println("Received chunk " + chunkNumber + " from download neighbor");
                }
                Thread.sleep(1000);
            }
        } catch (IOException | ClassNotFoundException |
                InterruptedException e) {
            e.printStackTrace();
        }
    }
}