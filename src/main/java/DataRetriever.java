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
                            select dish.id as dish_id, dish.name as dish_name, dish_type, dish.price as dish_price
                            from dish
                            where dish.id = ?;
                            """);
            preparedStatement.setInt(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                Dish dish = new Dish();
                dish.setId(resultSet.getInt("dish_id"));
                dish.setName(resultSet.getString("dish_name"));
                dish.setDishType(DishTypeEnum.valueOf(resultSet.getString("dish_type")));
                dish.setPrice(resultSet.getObject("dish_price") == null
                        ? null : resultSet.getDouble("dish_price"));
                dish.setDishIngredients(findDishIngredientById(id));
                return dish;
            }
            dbConnection.closeConnection(connection);
            throw new RuntimeException("Dish not found " + id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<DishIngredient> findDishIngredientById(Integer idDish) {
    DBConnection dbConnection = new DBConnection();
    Connection connection = dbConnection.getConnection();
    List<DishIngredient> dishIngredients = new ArrayList<>();
    try {
        PreparedStatement preparedStatement = connection.prepareStatement(
                """
                SELECT id_ingredient,ingredient.id,
                       ingredient.name,
                       ingredient.price,
                       ingredient.category,
                        DishIngredient.quantity_required 
                       from DishIngredient join Dish on Dish.id = id_dish
                          join Ingredient on Ingredient.id = id_ingredient
                          Where id_dish = ?;
                """
        );
        preparedStatement.setInt(1, idDish);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            Ingredient ingredient = new Ingredient();
            DishIngredient dishIngredient = new DishIngredient();
            ingredient.setId(resultSet.getInt("id_ingredient"));
            ingredient.setName(resultSet.getString("name"));
            ingredient.setPrice(resultSet.getDouble("price"));
            ingredient.setCategory(CategoryEnum.valueOf(resultSet.getString("category")));



            
            dishIngredient.setIngredient(ingredient);
            dishIngredient.setQuantityRequired(
                    resultSet.getDouble("required_quantity") 
            );

            dishIngredients.add(dishIngredient);
        }

        dbConnection.closeConnection(connection);
        return dishIngredients;

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
                        dish_type = EXCLUDED.dish_type,
                        selling_price = EXCLUDED.selling_price
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
                if (toSave.getSellingPrice() != null) {
                    ps.setDouble(2, toSave.getSellingPrice());
                } else {
                    ps.setNull(2, Types.DOUBLE);
                }
                ps.setString(3, toSave.getName());
                ps.setString(4, toSave.getDishType().name());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    dishId = rs.getInt(1);
                }
            }

            List<DishIngredient> newIngredients = toSave.getDishIngredients();
            detachDisIngredient(conn, dishId, newIngredients);
            savedIngredients(conn, dishId, newIngredients);
            attachDishIngredient(conn, dishId, newIngredients);

            conn.commit();
            return toSave;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void savedIngredients( Connection conn, Integer dishId, List<DishIngredient> newIngredients) {
        if (newIngredients == null || newIngredients.isEmpty()) {
            return;
        }
        String insertSql = """
                    INSERT INTO Ingredient (id, name, price, category)
                    VALUES (?, ?, ?, ?::ingredient_category)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        price = EXCLUDED.price,
                        category = EXCLUDED.category
                """;

        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (DishIngredient dishIngredient : newIngredients) {
                ps.setInt(1, dishIngredient.getIngredient().getId());
                ps.setString(2, dishIngredient.getIngredient().getName());
                ps.setDouble(3, dishIngredient.getIngredient().getPrice());
                ps.setString(4, dishIngredient.getIngredient().getCategory().name());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void attachDishIngredient(Connection conn, Integer dishId, List<DishIngredient> newIngredients) {
        if (newIngredients == null || newIngredients.isEmpty()) {
            return;
        }
        String insertDishIngredientSql = """
                    INSERT INTO DishIngredient (id, id_dish, id_ingredient, quantity_required, unit)
                    VALUES (?, ?, ?, ?, ?::unit_type)
                """;
        try (PreparedStatement ps = conn.prepareStatement(insertDishIngredientSql)) {
            for (DishIngredient dishIngredient : newIngredients) {
                ps.setInt(1, dishIngredient.getId());
                ps.setInt(2, dishIngredient.getDish().getId());
                ps.setInt(3, dishIngredient.getIngredient().getId());
                ps.setDouble(4, dishIngredient.getQuantityRequired());
                ps.setString(5, dishIngredient.getUnit().name());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void detachDisIngredient(Connection conn, Integer dishId, List<DishIngredient> newIngredients) {
        if (newIngredients == null || newIngredients.isEmpty()) {
            return;
        }
        String deleteDishSql = """
                    DELETE FROM DishIngredient WHERE id_dish = ?
                """;;

        try (PreparedStatement ps = conn.prepareStatement(deleteDishSql)) {
            ps.setInt(1, dishId);
            ps.executeUpdate();
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
                        INSERT INTO ingredient (id, name, category, price, required_quantity)
                        VALUES (?, ?, ?::ingredient_category, ?, ?)
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
                    if (ingredient.getQuantity() != null) {
                        ps.setDouble(5, ingredient.getQuantity());
                    }else {
                        ps.setNull(5, Types.DOUBLE);
                    }

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


    private void detachIngredients(Connection conn, Integer dishId, List<Ingredient> ingredients)
            throws SQLException {
        if (ingredients == null || ingredients.isEmpty()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE ingredient SET id_dish = NULL WHERE id_dish = ?")) {
                ps.setInt(1, dishId);
                ps.executeUpdate();
            }
            return;
        }

        String baseSql = """
                    UPDATE ingredient
                    SET id_dish = NULL
                    WHERE id_dish = ? AND id NOT IN (%s)
                """;

        String inClause = ingredients.stream()
                .map(i -> "?")
                .collect(Collectors.joining(","));

        String sql = String.format(baseSql, inClause);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, dishId);
            int index = 2;
            for (Ingredient ingredient : ingredients) {
                ps.setInt(index++, ingredient.getId());
            }
            ps.executeUpdate();
        }
    }

    private void attachIngredients(Connection conn, Integer dishId, List<Ingredient> ingredients)
            throws SQLException {

        if (ingredients == null || ingredients.isEmpty()) {
            return;
        }

        String attachSql = """
                    UPDATE ingredient
                    SET id_dish = ?
                    WHERE id = ?
                """;

        try (PreparedStatement ps = conn.prepareStatement(attachSql)) {
            for (Ingredient ingredient : ingredients) {
                ps.setInt(1, dishId);
                ps.setInt(2, ingredient.getId());
                ps.addBatch(); // Can be substitute ps.executeUpdate() but bad performance
            }
            ps.executeBatch();
        }
    }

    private List<Ingredient> findIngredientByDishId(Integer idDish) {
        DBConnection dbConnection = new DBConnection();
        Connection connection = dbConnection.getConnection();
        List<Ingredient> ingredients = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                            select ingredient.id, ingredient.name, ingredient.price, ingredient.category, ingredient.required_quantity
                            from ingredient where id_dish = ?;
                            """);
            preparedStatement.setInt(1, idDish);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Ingredient ingredient = new Ingredient();
                ingredient.setId(resultSet.getInt("id"));
                ingredient.setName(resultSet.getString("name"));
                ingredient.setPrice(resultSet.getDouble("price"));
                ingredient.setCategory(CategoryEnum.valueOf(resultSet.getString("category")));
                Object requiredQuantity = resultSet.getObject("required_quantity");
                ingredient.setQuantity(requiredQuantity == null ? null : resultSet.getDouble("required_quantity"));
                ingredients.add(ingredient);
            }
            dbConnection.closeConnection(connection);
            return ingredients;
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
                    "Any sequence found for " + tableName + "." + columnName
            );
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

    private void updateSequenceNextValue(Connection conn, String tableName, String columnName, String sequenceName) throws SQLException {
        String setValSql = String.format(
                "SELECT setval('%s', (SELECT COALESCE(MAX(%s), 0) FROM %s))",
                sequenceName, columnName, tableName
        );

        try (PreparedStatement ps = conn.prepareStatement(setValSql)) {
            ps.executeQuery();
        }
    }
}