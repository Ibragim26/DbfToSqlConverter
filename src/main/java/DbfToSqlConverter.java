import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


import com.linuxense.javadbf.DBFReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

public class DbfToSqlConverter {

    private Connection conn;
    private JdbcTemplate jdbcTemplate;

    public DbfToSqlConverter() throws SQLException {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/kladr");
        dataSource.setUsername("postgres");
        dataSource.setPassword("root");

        this.jdbcTemplate = new JdbcTemplate(dataSource);
        conn = dataSource.getConnection();
    }

    public void convert(String dbfFilePath, String tableName) throws IOException, SQLException {
        try {
            // Connect to the database
//            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb", "username", "password");
            conn.setAutoCommit(false);

            // Prepare the SQL statement
            PreparedStatement statement = conn.prepareStatement(String.format("INSERT INTO %s (LEVEL, SCNAME, SOCRNAME, KOD_T_ST) VALUES (?,?,?,?)", tableName));

            // Read the .dbf file

            FileInputStream fis = new FileInputStream(dbfFilePath);


            DBFReader reader = new DBFReader(fis, StandardCharsets.UTF_8);
            Object[] rowObjects;
            while ((rowObjects = reader.nextRecord()) != null) {
                statement.setString(1, rowObjects[0].toString());
                statement.setString(2, rowObjects[1].toString());
                statement.setString(3, rowObjects[2].toString());
                statement.setString(4, rowObjects[3].toString());
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
