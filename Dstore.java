import java.io.*;
import java.net.*;
import java.nio.file.Files;

public class Dstore {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String folder = args[3];

        File folderDir = new File(folder);
        if (!folderDir.exists()) {
            folderDir.mkdirs();
        }
        clearFolder(folderDir);
        Socket controller = new Socket("localhost", cport);
        
        PrintWriter controllerOut = new PrintWriter(controller.getOutputStream(), true);
        
        controllerOut.println(Protocol.JOIN_TOKEN + " " + port);
        System.out.println("Dstore joined on port: " + port);

        ServerSocket serverSocket = new ServerSocket(port);

        final PrintWriter finalControllerOut = controllerOut;

        Thread controllerThread = new Thread(() -> {
            try {
                handleControllerMessages(controller, folder, timeout);
            } catch (Exception e) {
                System.err.println("Controller message handler exited: " + e.getMessage());
            }
        });
        controllerThread.setDaemon(true);
        controllerThread.start();

        while (true) {
            try {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClient(client, finalControllerOut, folder)).start();
            } catch (IOException e) {
                System.err.println("Error accepting client connection: " + e.getMessage());
            }
        }
    }

    private static void clearFolder(File folder) {
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        file.delete();
                    }
                }
            }
        }
    }

    private static void handleControllerMessages(Socket controller, String folder, int timeout) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(controller.getInputStream()))) {

            PrintWriter controllerOut = new PrintWriter(controller.getOutputStream(), true);

            String command;
            while (true) {
                try {
                    controller.setSoTimeout(timeout);
                    command = in.readLine();

                    if (command == null) {
                        System.out.println("Controller connection closed");
                        break;
                    }

                    System.out.println("Received from Controller: " + command);

                    if (command.startsWith(Protocol.LIST_TOKEN)) {
                        handleListRequest(controllerOut, folder);
                    } else if (command.startsWith(Protocol.REMOVE_TOKEN)) {
                        handleRemoveRequest(controllerOut, command, folder);
                    }

                } catch (SocketTimeoutException e) {
                } catch (IOException e) {
                    System.err.println("Error reading from controller: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Controller communication error: " + e.getMessage());
        }
    }

    private static void handleListRequest(PrintWriter controllerOut, String folder) {
        try {
            File folderDir = new File(folder);
            File[] files = folderDir.listFiles();

            StringBuilder response = new StringBuilder(Protocol.LIST_TOKEN);
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        response.append(" ").append(file.getName());
                    }
                }
            }

            controllerOut.println(response.toString());
            System.out.println("Sent list to Controller: " + response.toString());
        } catch (Exception e) {
            System.err.println("Error handling list request: " + e.getMessage());
        }
    }

    private static void handleRemoveRequest(PrintWriter controllerOut, String command, String folder) {
        try {
            String[] parts = command.split(" ");
            String filename = parts[1];

            System.out.println("Removing file: " + filename);

            File file = new File(folder, filename);
            if (file.exists()) {
                boolean success = file.delete();
                System.out.println("File existed, delete result: " + success);
            } else {
                System.out.println("File did not exist");
            }

            controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
            System.out.println("Sent REMOVE_ACK to Controller for: " + filename);
        } catch (Exception e) {
            System.err.println("Error handling remove request: " + e.getMessage());
        }
    }

    private static void handleClient(Socket socket, PrintWriter controllerOut, String folder) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            OutputStream outStream = socket.getOutputStream();
            InputStream inStream = socket.getInputStream();
            
            String cmd = in.readLine();
            if (cmd == null) return;

            System.out.println("Received from Client: " + cmd);

            if (cmd.startsWith(Protocol.STORE_TOKEN)) {
                handleStoreRequest(cmd, out, inStream, controllerOut, folder);
            } else if (cmd.startsWith(Protocol.LOAD_DATA_TOKEN)) {
                handleLoadDataRequest(cmd, outStream, socket, folder);
            } else if (cmd.startsWith(Protocol.REMOVE_TOKEN)) {
                handleRemoveRequest(cmd, controllerOut, folder);
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Error closing socket: " + e.getMessage());
            }
        }
    }

    private static void handleStoreRequest(String cmd, PrintWriter out, InputStream inStream, 
                                          PrintWriter controllerOut, String folder) {
        try {
            String[] parts = cmd.split(" ");
            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            out.println(Protocol.ACK_TOKEN);

            File file = new File(folder, filename);
            if (file.exists()) {
                System.out.println("File already exists: " + filename);
                return;
            }

            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }

            byte[] buffer = new byte[filesize];
            int bytesRead = 0;
            int totalBytesRead = 0;
            
            try (FileOutputStream fos = new FileOutputStream(file)) {
                while (totalBytesRead < filesize) {
                    bytesRead = inStream.read(buffer, totalBytesRead, filesize - totalBytesRead);
                    if (bytesRead == -1) {
                        break;
                    }
                    totalBytesRead += bytesRead;
                }
                
                if (totalBytesRead == filesize) {
                    fos.write(buffer, 0, totalBytesRead);
                    controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                    System.out.println("Sent STORE_ACK to Controller for: " + filename);
                } else {
                    file.delete();
                    System.err.println("Incomplete file transfer for: " + filename);
                }
            }
        } catch (IOException e) {
            System.err.println("Error in store request: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Invalid filesize in store request: " + e.getMessage());
        }
    }

    private static void handleLoadDataRequest(String cmd, OutputStream outStream, Socket socket, String folder) {
        try {
            String[] parts = cmd.split(" ");
            String filename = parts[1];

            File file = new File(folder, filename);
            if (file.exists()) {
                try {
                    byte[] data = Files.readAllBytes(file.toPath());
                    outStream.write(data);
                    outStream.flush();
                    System.out.println("Sent file data for: " + filename);
                } catch (IOException e) {
                    System.err.println("Error reading or sending file: " + e.getMessage());
                    socket.close();
                }
            } else {
                System.out.println("File not found for LOAD_DATA: " + filename);
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error in load data request: " + e.getMessage());
        }
    }

    private static void handleRemoveRequest(String cmd, PrintWriter controllerOut, String folder) {
        try {
            String[] parts = cmd.split(" ");
            String filename = parts[1];

            System.out.println("Received REMOVE for file: " + filename);

            File file = new File(folder, filename);
            boolean fileExisted = file.exists();
            boolean deleteSuccess = false;

            if (fileExisted) {
                deleteSuccess = file.delete();
                System.out.println("File existed, delete result: " + deleteSuccess);
            } else {
                System.out.println("File did not exist");
            }

            controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
            System.out.println("Sent REMOVE_ACK to Controller for: " + filename);
        } catch (Exception e) {
            System.err.println("Error in remove request: " + e.getMessage());
        }
    }
}
