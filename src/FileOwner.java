import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FileOwner {
    public static final String FILE_NAME = "test.pdf";
    private static final int CHUNK_SIZE = 102400;
    private static Map<Integer, byte[]> chunks = new HashMap<>();
    private ServerSocket fileOwnerSocket;

    private static int portNumber;

    public FileOwner(int portNumber) {
        try {
            this.fileOwnerSocket = new ServerSocket(portNumber);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.splitFile();
        System.out.println("File owner is now accepting connections from Peers");
    }

    private void splitFile() {
        try {
            File fileOwnerFolder = new File("temp");
            if (!fileOwnerFolder.exists()) {
                fileOwnerFolder.mkdir();
            }
            FileInputStream fis = new FileInputStream(FILE_NAME);
            System.out.println("Splitting test file: " + FILE_NAME);
            byte[] maxChunkSize = new byte[CHUNK_SIZE];
            int sizeOfCurrentChunk;
            int chunkNumber = 0;
            while ((sizeOfCurrentChunk = fis.read(maxChunkSize)) != -1) //
            {
                byte[] chunkInBytes = Arrays.copyOfRange(maxChunkSize, 0, sizeOfCurrentChunk);
                chunks.put(chunkNumber, chunkInBytes);
                FileOutputStream fos = new FileOutputStream("temp/" + chunkNumber, false);
                fos.write(chunkInBytes);
                fos.flush();
                fos.close();
                chunkNumber++;
            }
            System.out.println("Splitting to chunks completed");
        } catch (IOException e) {
            System.out.println("Error: Invalid file name. Make sure the file " +
                    "is located in this folder.");
            System.exit(1);
        }
    }

    public void start() {
        try {
            while (true) {
                Socket socket = fileOwnerSocket.accept();
                FileOwnerHandler fileOwnerHandler = new FileOwnerHandler();
                fileOwnerHandler.storeChunks(chunks);
                fileOwnerHandler.openStreams(socket);
                fileOwnerHandler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1)
            throw new IllegalArgumentException("Run FileOwner with valid listening port");
        try {
            portNumber = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Argument passed must be a valid port number");
            System.exit(1);
        }
        new FileOwner(portNumber).start();
    }
}