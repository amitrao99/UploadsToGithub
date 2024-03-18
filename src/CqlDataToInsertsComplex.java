import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CqlDataToInsertsComplex {

    public static void main(String[] args) throws IOException {

        String describeTable = new String(Files.readAllBytes(Paths.get("describeTable.txt")));

        // Read the select output from a file, line by line
        List<String> selectOutput = Files.readAllLines(Paths.get("selectOutput.txt"));


        Map<String, String> columnTypes = extractColumnTypes(describeTable);

        // Generate INSERT statements
        List<String> insertStatements = generateInsertStatements("mykeyspace.mytable", columnTypes, selectOutput);

        // Output INSERT statements
        System.out.println("-- INSERT statements --");
        insertStatements.forEach(System.out::println);

        // Output CREATE TABLE statement (simplified)
        System.out.println("-- CREATE TABLE statement --");
        System.out.println(describeTable);
    }

    private static Map<String, String> extractColumnTypes(String describe) {
        Map<String, String> columnTypes = new HashMap<>();
        // Extract the portion of the string that contains column definitions
        String columnsPart = describe.split("\\(")[1].split("\\)")[0];
        // Split by comma, taking care not to split inside angle brackets (for generic types like list<text>)
        List<String> parts = Arrays.asList(columnsPart.split(",(?![^<>]*>)"));
        for (String part : parts) {
            String[] columnDetail = part.trim().split(" ", 2); // Split into 2 parts: name and type
            if (!columnDetail[1].trim().toLowerCase().contains("primary key")) {
                columnTypes.put(columnDetail[0].trim(), columnDetail[1].trim());
            }
        }
        return columnTypes;
    }

    private static List<String> generateInsertStatements(String tableName, Map<String, String> columnTypes, List<String> selectOutput) {
        List<String> inserts = new ArrayList<>();
        for (String row : selectOutput) {
            String[] values = row.split(" \\| ", -1);
            StringBuilder insert = new StringBuilder("INSERT INTO " + tableName + " (");
            insert.append(String.join(", ", columnTypes.keySet()));
            insert.append(") VALUES (");
            int i = 0;
            for (String columnName : columnTypes.keySet()) {
                String value = values[i++].trim();
                insert.append(formatValue(value, columnTypes.get(columnName)));
                if (i < values.length) {
                    insert.append(", ");
                }
            }
            insert.append(");");
            inserts.add(insert.toString());
        }
        return inserts;
    }

    private static String formatValue(String value, String type) {
        // For demonstration; extend this to handle all types and their edge cases
        switch (type.toLowerCase()) {
            case "int":
            case "bigint":
            case "float":
            case "double":
            case "boolean":
            case "date":
            case "timestamp":
                return value;
            case "text":
            case "varchar":
                return "'" + value.replace("'", "''") + "'";
            case "list<int>": // Simplified example for list of integers
            case "set<text>": // Simplified example for set of texts
            case "map<text, text>": // Simplified example for map of text to text
                return value;
            default:
                return value; // Placeholder for other types; customize as needed
        }
    }
}
