import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class NetworkNode {

    public static Integer leftNodePort = null;
    public static String leftNodeAddress = null;
    public static Integer serverPort = null;
    public static String identifier = null;
    public static String rightNodeFullAddress = null;
    public static ServerSocket serverSocket;

    public static Map<String, Long> nodeResources = new HashMap<>();
    public static Map<String, Long> motherNodeResourcesCopy = new HashMap<>();
    public static Map<String, Long> nodeResourcesCopy = new HashMap<>();

    public static String clientReturnString = "";

    public static void main(String[] args) throws IOException {
        String resources = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-ident":
                    identifier = args[++i];
                    break;
                case "-tcpport":
                    serverPort = Integer.parseInt(args[++i]);
                    break;
                case "-gateway":
                    rightNodeFullAddress = args[++i];
                    break;
                default:
                    if (resources == null) resources = args[i];
                    else if (!"TERMINATE".equals(resources)) resources += " " + args[i];
                    break;
            }
        }

        assertThatRequiredDataIsNotNull();
        allocateResourcesOnNodeLaunch(resources);

        serverSocket = new ServerSocket(serverPort);
        while (true) {

            if (rightNodeFullAddress == null) {
                trySetUpAndStartConnection(null, null);
            } else {
                trySetUpAndStartConnection(rightNodeFullAddress.split(":")[0], rightNodeFullAddress.split(":")[1]);
            }
        }

    }

    private static void trySetUpAndStartConnection(String address, String port) throws IOException {
        if (address == null && port == null) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                Thread newConnection = new ClientNodeHandler(socket, printWriter, bufferedReader);
                newConnection.start();
            } catch (Exception ex) {
                socket.close();
                ex.printStackTrace();
            }
        } else {
            Socket socket = new Socket(address, Integer.parseInt(port));
            try {
                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                Thread newConnection = new ClientNodeHandler(socket, printWriter, bufferedReader, "NODE:NODE " + serverPort);
                newConnection.start();

                while (true) {
                    trySetUpAndStartConnection(null, null);
                }

            } catch (IOException ex) {
                ex.printStackTrace();

            }
        }
    }

    private static void allocateResourcesOnNodeLaunch(String command) {
        if (command != null) {
            for (String singleResource : command.split(" ")) {
                String resourceName = singleResource.split(":")[0];
                String resourceSize = singleResource.split(":")[1];
                nodeResources.put(resourceName, Long.parseLong(resourceSize));
            }
        }
        nodeResourcesCopy = new HashMap<>(nodeResources);
        motherNodeResourcesCopy = new HashMap<>(nodeResources);
    }

    private static void assertThatRequiredDataIsNotNull() {
        assert serverPort != null : "No server port specified!";
        assert identifier != null : "No identifier specified!";
    }

    static class ClientNodeHandler extends Thread {

        final PrintWriter printWriter;
        final BufferedReader bufferedReader;
        final Socket socket;
        final String string;

        public ClientNodeHandler(Socket s, PrintWriter pw, BufferedReader br, String str) {
            socket = s;
            printWriter = pw;
            bufferedReader = br;
            string = str;
        }

        public ClientNodeHandler(Socket s, PrintWriter pw, BufferedReader br) {
            socket = s;
            printWriter = pw;
            bufferedReader = br;
            string = null;
        }

        @Override
        public void run() {
            String message;

            recogniseConnectionAndSendAddress();

            while (true) {
                try {
                    if (socket.isClosed()) {
                        break;
                    }
                    message = bufferedReader.readLine();
                    String messageCommand = message.split(" ")[0];

                    if (messageCommand.equals("NODE")) {
                        saveLeftNodeAddressAndCloseConnection(message);
                    } else {
                        allocate(message);
                    }
                    break;

                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
            closeEverything();
        }

        private Boolean saveLeftNodeAddressAndCloseConnection(String message) {
            leftNodePort = Integer.parseInt(message.split(" ")[1]);
            leftNodeAddress = socket.getInetAddress().toString().split("/")[1];
            assertThatNodeDataIsNotNull(leftNodePort, leftNodeAddress);
            return Boolean.TRUE;
        }

        private void assertThatNodeDataIsNotNull(Integer port, String address) {
            assert port != null : "No port assigned to node!";
            assert address != null : "No address assigned to node!";
        }

        private void recogniseConnectionAndSendAddress() {
            if (string != null) {

                String stringName = string.split(":")[0];
                String stringValue = string.split(":")[1];

                if (stringName.equals("NODE")) {
                    printWriter.write(stringValue);
                    printWriter.close();
                }

            }
        }

        private void allocate(String received) throws IOException {
            String[] identWithRes = received.split(" ");
            for (int i = 1; i < identWithRes.length; ++i) {
                String name = identWithRes[i].split(":")[0];
                String size = identWithRes[i].split(":")[1];
                if (nodeResourcesCopy.containsKey(name)) {
                    if (nodeResourcesCopy.get(name) >= Long.parseLong(size)) {
                        nodeResourcesCopy.replace(name, nodeResourcesCopy.get(name) - Long.parseLong(size));
                        clientReturnString += name+":"+size+":"+socket.getLocalAddress()+":"+serverPort;

                    } else  {
                        clientReturnString = "FAILED";
                    }

                }
            }


            printWriter.print(clientReturnString);
            printWriter.close();
            nodeResources = new HashMap<>(nodeResourcesCopy);
            motherNodeResourcesCopy = new HashMap<>(nodeResourcesCopy);
            clientReturnString = "";
        }




        private void closeEverything() {
            try {
                printWriter.close();
                bufferedReader.close();
                socket.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }

    }

}


