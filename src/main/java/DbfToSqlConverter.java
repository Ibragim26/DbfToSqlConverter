import com.linuxense.javadbf.DBFReader;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DbfToSqlConverter {

    private Connection conn;

    public DbfToSqlConverter() throws SQLException {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/kladr");
        dataSource.setUsername("postgres");
        dataSource.setPassword("root");

        conn = dataSource.getConnection();
    }

    public void convert(String dbfFilePath, String tableName, String config) throws IOException, SQLException {
        try {
            conn.setAutoCommit(false);

            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(tableName).append(" (");
            for (int i = 0; i < config.split(",").length - 1; i++) {
                sb.append(config.split(",")[i]).append(",");
            }
            sb.append(config.split(",")[config.split(",").length - 1]).append(") VALUES ( ");
            for (int i = 0; i < config.split(",").length - 1; i++) {
                sb.append("?,");
            }
            sb.append("?)");

            PreparedStatement statement = conn.prepareStatement(sb.toString());

            FileInputStream fis = new FileInputStream(dbfFilePath);


            DBFReader reader = new DBFReader(fis, StandardCharsets.UTF_8);
            Object[] rowObjects;
            while ((rowObjects = reader.nextRecord()) != null) {
                for (int i = 0; i < config.split(",").length; i++) {
                    statement.setString(i+1, rowObjects[i].toString());
                }
                statement.addBatch();
            }

            // Execute the SQL statement
            statement.executeBatch();
            conn.commit();

            // Close the connection
            statement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
