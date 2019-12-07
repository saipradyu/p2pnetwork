import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PeerHandler extends Thread {
    private ConcurrentHashMap<Integer, byte[]> chunkList;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;

    private void processMessages() {
        while (true) {
            Object quantity;
            while (true) {
                try {
                    quantity = inputStream.readObject();
                    assert (quantity instanceof String);
                    break;
                } catch (ClassNotFoundException | IOException e) {
                    e.printStackTrace();
                }
            }
            String message = (String) quantity;
            switch (message) {
                case "CHUNK_LIST":
                    sendChunkIdList();
                    break;
                case "CHECK_NEIGHBOR":
                    sendCheckNeighbor();
                    break;
                case "REQUEST_CHUNK":
                    sendRequestedChunk();
                    break;
                default:
                    break;
            }
        }
    }

    private void sendChunkIdList() {
        List<Integer> peerChunkList = new ArrayList<>(chunkList.keySet());
        Collections.sort(peerChunkList);
        try {
            System.out.println("Chunk list requested from upload neighbor");
            transfer(peerChunkList);
            System.out.println("Chunk list sent to upload neighbor");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendCheckNeighbor() {
        try {
            int chunkNumber = inputStream.readInt();
            if (chunkList.containsKey(chunkNumber)) {
                transfer(1);
            } else {
                transfer(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void sendRequestedChunk() {
        try {
            int chunkNumber = inputStream.readInt(); //Read the requested chunk
            System.out.println("Upload neighbor requested chunk " +
                    chunkNumber);

            transfer(chunkNumber);
            transfer(chunkList.get(chunkNumber));
            System.out.println("Sent chunk " + chunkNumber + " to upload neighbor");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initIOStreams(Socket socket) throws IOException {
        outputStream = new ObjectOutputStream(socket.getOutputStream());
        inputStream = new ObjectInputStream(socket.getInputStream());
    }

    public void storeChunks(ConcurrentHashMap<Integer, byte[]> chunks) {
        this.chunkList = chunks;
    }

    private void transfer(Object message) throws IOException {
        outputStream.writeObject(message);
        outputStream.flush();
        outputStream.reset();
    }

    private void transfer(int message) throws IOException {
        outputStream.writeInt(message);
        outputStream.flush();
        outputStream.reset();
    }

    @Override
    public void run() {
        processMessages();
    }
}