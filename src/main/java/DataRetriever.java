import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataRetriever {
    Dish findDishById(Integer id) {
        DBConnection dbConnection = new DBConnection();
        Connection connection = dbConnection.getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                            select i.id, i.name, i.price, i.category,
                            di.quantity_required, di.unit
                            FROM DishIngredient di
                            JOIN Ingredient i ON di.id_ingredient = i.id
                            WHERE di.id_dish = ?
                                                """);
            preparedStatement.setInt(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                do {
                    Ingredient ingredients = new Ingredient();
                    ingredients.setId(resultSet.getInt("id"));
                    ingredients.setName(resultSet.getString("name"));
                    ingredients.setCategory(CategoryEnum.valueOf(resultSet.getString("category")));
                    ingredients.setPrice(resultSet.getDouble("price"));

                    DishIngredient dishIngredient = new DishIngredient();
                    dishIngredient.setIngredient(ingredients);
                    dishIngredient.setQuantityRequired(resultSet.getDouble("quantity_required"));
                    dishIngredient.setUnit(UnitType.valueOf(resultSet.getString("unit")));
                } while (
                    resultSet.next()
                );
            }
            dbConnection.closeConnection(connection);
            throw new RuntimeException("Dish not found " + id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    Dish saveDish(Dish toSave) {
        String upsertDishSql = """
                    INSERT INTO dish (id, price, name, dish_type)
                    VALUES (?, ?, ?, ?::dish_type)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        dish_type = EXCLUDED.dish_type
                    RETURNING id
                """;

        try (Connection conn = new DBConnection().getConnection()) {
            conn.setAutoCommit(false);
            Integer dishId;
            try (PreparedStatement ps = conn.prepareStatement(upsertDishSql)) {
                if (toSave.getId() != null) {
                    ps.setInt(1, toSave.getId());
                } else {
                    ps.setInt(1, getNextSerialValue(conn, "dish", "id"));
                }
                if (toSave.getPrice() != null) {
                    ps.setDouble(2, toSave.getPrice());
                } else {
                    ps.setNull(2, Types.DOUBLE);
                }
                ps.setString(3, toSave.getName());
                ps.setString(4, toSave.getDishType().name());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    dishId = rs.getInt(1);
                }

                String insertDishIngredient = """
                        insert into DishIngredient (id_dish, id_ingredient, quantity_required,unit)
                        values (?,?,?,?::unit_type)
                        """;

                try (PreparedStatement psDishIngredient = conn.prepareStatement(insertDishIngredient)) {
                    for (DishIngredient dishIngredient : toSave.getDishIngredients()) {
                        psDishIngredient.setInt(1, dishId);
                        psDishIngredient.setInt(2, dishIngredient.getIngredient().getId());
                        psDishIngredient.setDouble(3, dishIngredient.getQuantityRequired());
                        psDishIngredient.setString(4, dishIngredient.getUnit().name());
                        psDishIngredient.executeUpdate();
                    }
                }
            }

            conn.commit();
            return findDishById(dishId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Ingredient> createIngredients(List<Ingredient> newIngredients) {
        if (newIngredients == null || newIngredients.isEmpty()) {
            return List.of();
        }
        List<Ingredient> savedIngredients = new ArrayList<>();
        DBConnection dbConnection = new DBConnection();
        Connection conn = dbConnection.getConnection();
        try {
            conn.setAutoCommit(false);
            String insertSql = """
                        INSERT INTO ingredient (id, name, category, price)
                        VALUES (?, ?, ?::ingredient_category, ?)
                        RETURNING id
                    """;
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                for (Ingredient ingredient : newIngredients) {
                    if (ingredient.getId() != null) {
                        ps.setInt(1, ingredient.getId());
                    } else {
                        ps.setInt(1, getNextSerialValue(conn, "ingredient", "id"));
                    }
                    ps.setString(2, ingredient.getName());
                    ps.setString(3, ingredient.getCategory().name());
                    ps.setDouble(4, ingredient.getPrice());

                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        int generatedId = rs.getInt(1);
                        ingredient.setId(generatedId);
                        savedIngredients.add(ingredient);
                    }
                }
                conn.commit();
                return savedIngredients;
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            dbConnection.closeConnection(conn);
        }
    }


    private List<DishIngredient> findDishIngredientByDishId(Integer dishId) {
        DBConnection dbConnection = new DBConnection();
        Connection connection = dbConnection.getConnection();
        List<DishIngredient> DishIngredients = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                        select di.id as di_id,di.quantity_required,di.unit,i.id 
                        as ingredient_id,i.name,i.price,i.category
                        from DishIngredient di
                        join Ingredient i ON di.id_ingredient = i.id
                        where di.id_dish = ?
                            """);
            preparedStatement.setInt(1, dishId);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Ingredient ingredient = new Ingredient();
                ingredient.setId(resultSet.getInt("ingredient_id"));
                ingredient.setName(resultSet.getString("name"));
                ingredient.setPrice(resultSet.getDouble("price"));
                ingredient.setCategory(CategoryEnum.valueOf(resultSet.getString("category")));
                
                DishIngredient dishIngredient = new DishIngredient();
                dishIngredient.setId(resultSet.getInt("di_id"));
                dishIngredient.setQuantityRequired(resultSet.getDouble("quantity_required"));
                dishIngredient.setUnit(UnitType.valueOf(resultSet.getString("unit")));
                dishIngredient.setIngredient(ingredient);
                DishIngredients.add(dishIngredient);
            }
            dbConnection.closeConnection(connection);
            return DishIngredients;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSerialSequenceName(Connection conn, String tableName, String columnName)
            throws SQLException {

        String sql = "SELECT pg_get_serial_sequence(?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setString(2, columnName);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        return null;
    }

    private int getNextSerialValue(Connection conn, String tableName, String columnName)
            throws SQLException {

        String sequenceName = getSerialSequenceName(conn, tableName, columnName);
        if (sequenceName == null) {
            throw new IllegalArgumentException(
                    "Any sequence found for " + tableName + "." + columnName);
        }
        updateSequenceNextValue(conn, tableName, columnName, sequenceName);

        String nextValSql = "SELECT nextval(?)";

        try (PreparedStatement ps = conn.prepareStatement(nextValSql)) {
            ps.setString(1, sequenceName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    private void updateSequenceNextValue(Connection conn, String tableName, String columnName, String sequenceName)
            throws SQLException {
        String setValSql = String.format(
                "SELECT setval('%s', (SELECT COALESCE(MAX(%s), 0) FROM %s))",
                sequenceName, columnName, tableName);

        try (PreparedStatement ps = conn.prepareStatement(setValSql)) {
            ps.executeQuery();
        }
    }
}
