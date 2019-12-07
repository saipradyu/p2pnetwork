import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class FileOwnerHandler extends Thread {
    // Key => Chunk numbers, Value => Bytes that comprise the chunks
    private Map<Integer, byte[]> chunks;
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
                } catch (Exception ignored) {
                }
            }
            String message = (String) quantity;

            // Process message depending on its type
            switch (message) {
                case "FILE_NAME":
                    sendFileName();
                    break;
                case "CHUNK_LIST":
                    sendChunkIdList();
                    break;
                case "REQUEST_CHUNK":
                    sendRequestedChunk();
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Send the file name.
     */
    private void sendFileName() {
        try {
            transferObject(FileOwner.FILE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send the chunk ID list.
     */
    private void sendChunkIdList() {
        // Extract the master chunk ID list from the chunks map
        List<Integer> masterChunkIdList = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            if (chunks.containsKey(i)) {
                masterChunkIdList.add(i);
            }
        }

        // Transfer the master chunk ID list
        try {
            int peerId = inputStream.readInt();
            System.out.println("Chunk list requested from Peer " + peerId);
            transferObject(masterChunkIdList);
            System.out.println("Chunk list sent to Peer " + peerId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send the requested chunk.
     */
    private void sendRequestedChunk() {
        try {
            int chunkNumber = inputStream.readInt();
            int peerId = inputStream.readInt();

            System.out.println("Chunk " + chunkNumber + " requested by Peer " + peerId);
            transfer(chunkNumber);
            transferObject(chunks.get(chunkNumber));

            System.out.println("Sent chunk " + chunkNumber + " to Peer " + peerId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Store chunk information.
     *
     * @param chunks with ChunkIndex as key and the chunk bytes as value
     */
    void storeChunks(Map<Integer, byte[]> chunks) {
        this.chunks = chunks;
    }

    void openStreams(Socket socket) throws IOException {
        outputStream = new ObjectOutputStream(socket.getOutputStream());
        inputStream = new ObjectInputStream(socket.getInputStream());
    }

    private void transferObject(Object message) throws IOException {
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