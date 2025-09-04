import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

// Si usas Maven, agrega estas dependencias a tu pom.xml:
// <dependencies>
//     <!-- MySQL Connector/J -->
//     <dependency>
//         <groupId>mysql</groupId>
//         <artifactId>mysql-connector-java</artifactId>
//         <version>8.0.33</version>
//     </dependency>
//     <!-- Apache Commons CSV -->
//     <dependency>
//         <groupId>org.apache.commons</groupId>
//         <artifactId>commons-csv</artifactId>
//         <version>1.10.0</version>
//     </dependency>
// </dependencies>

public class Main {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/Entregable1";
    private static final String USER = "root";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        try {
            // 1. Crear el esquema de la base de datos
            createSchema();
            System.out.println("Esquema de la base de datos creado exitosamente.");

            // 2. Cargar los datos desde los archivos CSV
            loadData();
            System.out.println("Datos cargados exitosamente.");

            System.out.println("----------------------------------------");

            // 3. Obtener el producto que más recaudó
            findMostProfitableProduct();
            System.out.println("----------------------------------------");

            // 4. Imprimir la lista de clientes ordenada por facturación
            listClientsByRevenue();

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Crea las tablas en la base de datos.
     * @throws SQLException si ocurre un error en la conexión o consulta SQL.
     */
    public static void createSchema() throws SQLException {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // SQL para crear las tablas
            String createClientesTable = "CREATE TABLE IF NOT EXISTS clientes ("
                    + "id_cliente INT PRIMARY KEY,"
                    + "nombre VARCHAR(255)"
                    + ")";

            String createProductosTable = "CREATE TABLE IF NOT EXISTS productos ("
                    + "id_producto INT PRIMARY KEY,"
                    + "nombre VARCHAR(255),"
                    + "valor DECIMAL(10, 2)"
                    + ")";

            String createFacturasTable = "CREATE TABLE IF NOT EXISTS facturas ("
                    + "id_factura INT PRIMARY KEY,"
                    + "id_cliente INT,"
                    + "FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)"
                    + ")";

            String createFacturasProductosTable = "CREATE TABLE IF NOT EXISTS facturas_productos ("
                    + "id_factura INT,"
                    + "id_producto INT,"
                    + "cantidad INT,"
                    + "FOREIGN KEY (id_factura) REFERENCES facturas(id_factura),"
                    + "FOREIGN KEY (id_producto) REFERENCES productos(id_producto)"
                    + ")";

            // Ejecutar las consultas de creación de tablas
            stmt.execute(createClientesTable);
            stmt.execute(createProductosTable);
            stmt.execute(createFacturasTable);
            stmt.execute(createFacturasProductosTable);
        }
    }

    /**
     * Carga los datos desde los archivos CSV a las tablas.
     * @throws SQLException si ocurre un error en la conexión o consulta SQL.
     * @throws IOException si ocurre un error al leer los archivos.
     */
    public static void loadData() throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD)) {
            // Cargar clientes.csv
            String insertClienteSQL = "INSERT INTO clientes (id_cliente, nombre) VALUES (?, ?)";
            loadCsv("clientes.csv", insertClienteSQL, conn, new int[]{1, 2});

            // Cargar productos.csv
            String insertProductoSQL = "INSERT INTO productos (id_producto, nombre, valor) VALUES (?, ?, ?)";
            loadCsv("productos.csv", insertProductoSQL, conn, new int[]{1, 2, 3});

            // Cargar facturas.csv
            String insertFacturaSQL = "INSERT INTO facturas (id_factura, id_cliente) VALUES (?, ?)";
            loadCsv("facturas.csv", insertFacturaSQL, conn, new int[]{1, 2});
            
            // Cargar facturas-productos.csv
            String insertFacturaProductoSQL = "INSERT INTO facturas_productos (id_factura, id_producto, cantidad) VALUES (?, ?, ?)";
            loadCsv("facturas-productos.csv", insertFacturaProductoSQL, conn, new int[]{1, 2, 3});
        }
    }
    
    /**
     * Helper para leer un CSV y cargar datos.
     * @param filePath Ruta al archivo CSV.
     * @param sql La consulta SQL de inserción.
     * @param conn La conexión a la base de datos.
     * @param columnIndices Los índices de las columnas a usar para la inserción.
     * @throws IOException si ocurre un error al leer el archivo.
     * @throws SQLException si ocurre un error con la consulta.
     */
    private static void loadCsv(String filePath, String sql, Connection conn, int[] columnIndices) throws IOException, SQLException {
        try (CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(new FileReader(filePath));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            System.out.println("Cargando datos desde: " + filePath);
            for (CSVRecord record : parser) {
                // Configurar los parámetros del PreparedStatement dinámicamente
                for (int i = 0; i < columnIndices.length; i++) {
                    int colIndex = columnIndices[i];
                    String value = record.get(colIndex - 1);
                    if (colIndex == 3 && filePath.equals("productos.csv")) {
                        pstmt.setBigDecimal(i + 1, new java.math.BigDecimal(value));
                    } else {
                        pstmt.setString(i + 1, value);
                    }
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            System.out.println("Carga de " + filePath + " completada.");
        }
    }

    /**
     * Retorna el producto que más recaudó.
     * @throws SQLException si ocurre un error en la consulta SQL.
     */
    public static void findMostProfitableProduct() throws SQLException {
        String query = "SELECT p.nombre, SUM(fp.cantidad * p.valor) AS recaudacion "
                + "FROM productos p "
                + "JOIN facturas_productos fp ON p.id_producto = fp.id_producto "
                + "GROUP BY p.nombre "
                + "ORDER BY recaudacion DESC "
                + "LIMIT 1";

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            System.out.println("Producto que más recaudó:");
            if (rs.next()) {
                String nombreProducto = rs.getString("nombre");
                double recaudacion = rs.getDouble("recaudacion");
                System.out.printf("Producto: %s, Recaudación: $%.2f%n", nombreProducto, recaudacion);
            } else {
                System.out.println("No se encontraron productos.");
            }
        }
    }

    /**
     * Imprime una lista de clientes, ordenada por a cuál se le facturó más.
     * @throws SQLException si ocurre un error en la consulta SQL.
     */
    public static void listClientsByRevenue() throws SQLException {
        String query = "SELECT c.nombre, SUM(fp.cantidad * p.valor) AS total_facturado "
                + "FROM clientes c "
                + "JOIN facturas f ON c.id_cliente = f.id_cliente "
                + "JOIN facturas_productos fp ON f.id_factura = fp.id_factura "
                + "JOIN productos p ON fp.id_producto = p.id_producto "
                + "GROUP BY c.nombre "
                + "ORDER BY total_facturado DESC";

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            System.out.println("Lista de clientes ordenada por facturación:");
            while (rs.next()) {
                String nombreCliente = rs.getString("nombre");
                double totalFacturado = rs.getDouble("total_facturado");
                System.out.printf("Cliente: %s, Total Facturado: $%.2f%n", nombreCliente, totalFacturado);
            }
        }
    }
}
