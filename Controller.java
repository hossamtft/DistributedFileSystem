import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {
    private static final String STATUS_STORE_IN_PROGRESS = "store in progress";
    private static final String STATUS_STORE_COMPLETE = "store complete";
    private static final String STATUS_REMOVE_IN_PROGRESS = "remove in progress";
    
    public static void main(String[] args) throws Exception {
        int cport = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        new Controller().start(cport, R, timeout, rebalancePeriod);
    }

    private ServerSocket serverSocket;
    private final Map<Integer, Socket> dstores = new ConcurrentHashMap<>();
    private final Map<String, FileEntry> fileIndex = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> reloadTracker = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> storeAcks = new ConcurrentHashMap<>();
    private final Map<String, Socket> storeClients = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private int R, timeout, rebalancePeriod;

    public void start(int cport, int R, int timeout, int rebalancePeriod) throws IOException {
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        serverSocket = new ServerSocket(cport);
        System.out.println("Controller started on port " + cport);
        
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    checkDstoreConnections();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();
        
        while (true) {
            Socket client = serverSocket.accept();
            new Thread(() -> handleConnection(client)).start();
        }
    }

    private void checkDstoreConnections() {
        List<Integer> disconnectedDstores = new ArrayList<>();
        
        for (Map.Entry<Integer, Socket> entry : dstores.entrySet()) {
            int port = entry.getKey();
            Socket socket = entry.getValue();
            
            if (socket.isClosed() || !socket.isConnected()) {
                disconnectedDstores.add(port);
            } else {
                try {
                    socket.getOutputStream().write(0);
                } catch (IOException e) {
                    disconnectedDstores.add(port);
                }
            }
        }
        
        synchronized (lock) {
            for (int port : disconnectedDstores) {
                dstores.remove(port);
                System.out.println("Dstore disconnected on port: " + port);
            }
            
            if (!disconnectedDstores.isEmpty()) {
                updateFileIndexAfterDstoreDisconnection(disconnectedDstores);
            }
        }
    }

    private void updateFileIndexAfterDstoreDisconnection(List<Integer> disconnectedDstores) {
        for (Map.Entry<String, FileEntry> entry : fileIndex.entrySet()) {
            String filename = entry.getKey();
            FileEntry fileEntry = entry.getValue();
            List<Integer> currentDstores = fileEntry.dstores;
            
            currentDstores.removeAll(disconnectedDstores);
            
            if (currentDstores.size() < R && STATUS_STORE_COMPLETE.equals(fileEntry.status)) {
                System.out.println("File " + filename + " is under-replicated after Dstore disconnection");
            }
        }
    }

    private void handleConnection(Socket socket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            String command;
            while ((command = in.readLine()) != null) {
                if (command.startsWith(Protocol.JOIN_TOKEN)) {
                    int port = Integer.parseInt(command.split(" ")[1]);
                    dstores.put(port, socket);
                    System.out.println("Dstore joined on port: " + port);
                } else if (command.startsWith(Protocol.STORE_ACK_TOKEN)) {
                    String filename = command.split(" ")[1];
                    int dstorePort = socket.getPort();
                    synchronized (lock) {
                        storeAcks.putIfAbsent(filename, new HashSet<>());
                        storeAcks.get(filename).add(dstorePort);
                        if (storeAcks.get(filename).size() == R) {
                            Socket client = storeClients.get(filename);
                            FileEntry entry = fileIndex.get(filename);
                            if (entry != null) {
                                entry.status = STATUS_STORE_COMPLETE;
                            }
                            if (client != null && !client.isClosed()) {
                                PrintWriter clientOut = new PrintWriter(client.getOutputStream(), true);
                                clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                            }
                            storeAcks.remove(filename);
                            storeClients.remove(filename);
                        }
                    }
                } else {
                    handleClientRequest(command, in, out, socket);
                }
            }

            for (Map.Entry<Integer, Socket> entry : new HashMap<>(dstores).entrySet()) {
                if (entry.getValue() == socket) {
                    dstores.remove(entry.getKey());
                    System.out.println("Dstore disconnected on port: " + entry.getKey());
                }
            }
            socket.close();
        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
            try {
                for (Map.Entry<Integer, Socket> entry : new HashMap<>(dstores).entrySet()) {
                    if (entry.getValue() == socket) {
                        dstores.remove(entry.getKey());
                        System.out.println("Dstore disconnected due to error on port: " + entry.getKey());
                    }
                }
                socket.close();
            } catch (IOException closeError) {
                System.err.println("Error closing socket: " + closeError.getMessage());
            }
        }
    }

    private void handleClientRequest(String command, BufferedReader in, PrintWriter out, Socket socket) throws IOException {
        String[] parts = command.split(" ");
        String clientId = socket.getRemoteSocketAddress().toString();
        String op = parts[0];
        switch (op) {
            case Protocol.LIST_TOKEN -> {
                reloadTracker.remove(clientId);
                handleList(out);
            }
            case Protocol.STORE_TOKEN -> {
                reloadTracker.remove(clientId);
                handleStore(parts[1], Integer.parseInt(parts[2]), out, socket);
            }
            case Protocol.LOAD_TOKEN -> {
                reloadTracker.remove(clientId);
                handleLoad(parts[1], out, clientId);
            }
            case Protocol.RELOAD_TOKEN -> handleReload(parts[1], out, clientId);
            case Protocol.REMOVE_TOKEN -> {
                reloadTracker.remove(clientId);
                handleRemove(parts[1], out);
            }
            default -> {
                System.err.println("Unknown command received: " + command);
            }
        }
    }

    private void handleStoreTimeout(String filename) {
        synchronized (lock) {
            FileEntry entry = fileIndex.get(filename);
            if (entry != null && entry.status.equals(STATUS_STORE_IN_PROGRESS)) {

                fileIndex.remove(filename);

                storeAcks.remove(filename);

                Socket client = storeClients.get(filename);
                if (client != null && !client.isClosed()) {
                    try {
                        PrintWriter clientOut = new PrintWriter(client.getOutputStream(), true);
                        clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    } catch (IOException e) {
                        System.err.println("Error notifying client of store timeout: " + e.getMessage());
                    }
                }
                storeClients.remove(filename);
            }
        }
    }

    private void handleStore(String filename, int filesize, PrintWriter out, Socket clientSocket) {
        synchronized (lock) {
            FileEntry existingEntry = fileIndex.get(filename);
            
            if (existingEntry != null) {
                if (STATUS_STORE_COMPLETE.equals(existingEntry.status)) {
                    out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    return;
                } else if (STATUS_STORE_IN_PROGRESS.equals(existingEntry.status)) {
                    List<Integer> selectedPorts = existingEntry.dstores;
                    StringBuilder sb = new StringBuilder(Protocol.STORE_TO_TOKEN);
                    for (int p : selectedPorts) sb.append(" ").append(p);
                    out.println(sb.toString());
    
                    storeClients.put(filename, clientSocket);
                    storeAcks.put(filename, new HashSet<>());
    
                    new Thread(() -> {
                        try {
                            Thread.sleep(timeout);
                            handleStoreTimeout(filename);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }).start();
                    return;
                }
            }
    
            if (dstores.size() < R) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
    
            List<Integer> selectedPorts = new ArrayList<>(dstores.keySet()).subList(0, R);
            fileIndex.put(filename, new FileEntry(filesize, selectedPorts, STATUS_STORE_IN_PROGRESS));
    
            StringBuilder sb = new StringBuilder(Protocol.STORE_TO_TOKEN);
            for (int p : selectedPorts) sb.append(" ").append(p);
            out.println(sb.toString());
    
            storeAcks.put(filename, new HashSet<>());
            storeClients.put(filename, clientSocket);
    
            new Thread(() -> {
                try {
                    Thread.sleep(timeout);
                    handleStoreTimeout(filename);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }
    
    private void handleLoad(String filename, PrintWriter out, String clientId) {
        synchronized (lock) {
            FileEntry entry = fileIndex.get(filename);
            if (entry == null || !entry.status.equals(STATUS_STORE_COMPLETE)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
            
            reloadTracker.putIfAbsent(clientId, new HashSet<>());
            boolean dstoreFound = false;
            for (int port : entry.dstores) {
                if (!reloadTracker.get(clientId).contains(port)) {
                    reloadTracker.get(clientId).add(port);
                    out.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + entry.size);
                    dstoreFound = true;
                    break;
                }
            }
            
            if (!dstoreFound) {
                out.println(Protocol.ERROR_LOAD_TOKEN);
                reloadTracker.remove(clientId);
            }
        }
    }

    private void handleReload(String filename, PrintWriter out, String clientId) {
        handleLoad(filename, out, clientId);
    }

    private void handleRemove(String filename, PrintWriter out) {
        synchronized (lock) {
            FileEntry entry = fileIndex.get(filename);
            if (entry == null || !entry.status.equals(STATUS_STORE_COMPLETE)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
            
            entry.status = STATUS_REMOVE_IN_PROGRESS;
            
            Set<Integer> removeAcks = new HashSet<>();
            
            for (int port : entry.dstores) {
                try {
                    if (dstores.containsKey(port)) {
                        PrintWriter dOut = new PrintWriter(dstores.get(port).getOutputStream(), true);
                        dOut.println(Protocol.REMOVE_TOKEN + " " + filename);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            final int expectedAcks = entry.dstores.size();
            
            new Thread(() -> {
                try {
                    Thread.sleep(timeout);
                    
                    synchronized (lock) {
                        if (removeAcks.size() < expectedAcks) {
                            System.out.println("Remove timeout for: " + filename);
                        }
                        
                        fileIndex.remove(filename);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
            
            out.println(Protocol.REMOVE_COMPLETE_TOKEN);
        }
    }

    private void handleList(PrintWriter out) {
        if (dstores.size() < R) {
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        synchronized (lock) {
            StringBuilder sb = new StringBuilder(Protocol.LIST_TOKEN);
            for (Map.Entry<String, FileEntry> entry : fileIndex.entrySet()) {
                if (entry.getValue().status.equals(STATUS_STORE_COMPLETE))
                    sb.append(" ").append(entry.getKey());
            }
            out.println(sb.toString());
        }
    }

    private void safeUpdateFileIndex(String filename, FileEntry entry) {
        synchronized (lock) {
            fileIndex.put(filename, entry);
        }
    }

    private FileEntry safeGetFileEntry(String filename) {
        synchronized (lock) {
            return fileIndex.get(filename);
        }
    }

    private void safeRemoveFileFromIndex(String filename) {
        synchronized (lock) {
            fileIndex.remove(filename);
        }
    }

    static class FileEntry {
        int size;
        List<Integer> dstores;
        String status;

        FileEntry(int size, List<Integer> dstores, String status) {
            this.size = size;
            this.dstores = dstores;
            this.status = status;
        }
    }
}
