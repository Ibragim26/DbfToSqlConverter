import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import java.io.IOException;
import java.sql.SQLException;


public class Main {


    private static DbfToSqlConverter converter;

    static {
        try {
            converter = new DbfToSqlConverter();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public Main() throws SQLException {
    }

    public static void main(String[] args) throws IOException, SQLException {
        converter.convert("C:\\Users\\Lance\\Desktop\\kladr\\SOCRBASE.dbf", "socrbase", "LEVEL, SCNAME, SOCRNAME, KOD_T_ST");
    }
}
