import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataRetriever {
    public Dish findDishById(Integer id) {
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
                        ? null
                        : resultSet.getDouble("dish_price"));
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
                            """);
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
                        resultSet.getDouble("quantity_required"));

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

    private void savedIngredients(Connection conn, Integer dishId, List<DishIngredient> newIngredients) {
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
                """;
        ;

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
                        INSERT INTO ingredient (id, name, price, category)
                        VALUES (?, ?, ?, ?::ingredient_category, ?)
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
                    ps.setDouble(3, ingredient.getPrice());
                    ps.setString(4, ingredient.getCategory().name());
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

    public Ingredient findIngredientById(Integer id) {
        DBConnection dbConnection = new DBConnection();
        try (Connection connection = dbConnection.getConnection()) {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("select id, name, price, category from ingredient where id = ?;");
            preparedStatement.setInt(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int idIngredient = resultSet.getInt("id");
                String name = resultSet.getString("name");
                CategoryEnum category = CategoryEnum.valueOf(resultSet.getString("category"));
                Double price = resultSet.getDouble("price");
                return new Ingredient(idIngredient, name, category, price,
                        findStockMovementsByIngredientId(idIngredient));
            }
            throw new RuntimeException("Ingredient not found " + id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<StockMovement> findStockMovementsByIngredientId(Integer ingredientId) {
        DBConnection dbConnection = new DBConnection();
        List<StockMovement> movements = new ArrayList<>();

        try (Connection conn = dbConnection.getConnection()) {
            PreparedStatement stmt = conn.prepareStatement("""
                        SELECT id, quantity, unit, type, creation_datetime
                        FROM StockMovement
                        WHERE id_ingredient = ?;
                    """);

            stmt.setInt(1, ingredientId);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                StockValue value = new StockValue(
                        rs.getDouble("quantity"),
                        UnitType.valueOf(rs.getString("unit")));

                Timestamp ts = rs.getTimestamp("creation_datetime");
                Instant creationInstant = ts != null ? ts.toInstant() : Instant.now();

                StockMovement movement = new StockMovement(
                        rs.getInt("id"),
                        value,
                        MovementTypeEnum.valueOf(rs.getString("type")),
                        creationInstant);

                movements.add(movement);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "Error while fetching stock movements for ingredient id " + ingredientId,
                    e);
        }

        return movements;
    }

    public StockValue calculateStock(Ingredient ingredient) {
        double totalQuantity = 0.0;
        UnitType unit = UnitType.KG;

        if (ingredient.getStockMovementList() != null) {
            for (StockMovement movement : ingredient.getStockMovementList()) {
                if (movement.getType() == MovementTypeEnum.IN) {
                    totalQuantity += movement.getValue().getQuantity();
                } else {
                    totalQuantity -= movement.getValue().getQuantity();
                }
            }
        }

        return new StockValue(totalQuantity, unit);
    }

    public Ingredient saveIngredient(Ingredient toSave) {
        String upsertIngredientSql = """
                    INSERT INTO ingredient (id, name, price, category)
                    VALUES (?, ?, ?, ?::dish_type)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        price = EXCLUDED.price
                    RETURNING id
                """;

        try (Connection conn = new DBConnection().getConnection()) {
            conn.setAutoCommit(false);
            Integer ingredientId;
            try (PreparedStatement ps = conn.prepareStatement(upsertIngredientSql)) {
                if (toSave.getId() != null) {
                    ps.setInt(1, toSave.getId());
                } else {
                    ps.setInt(1, getNextSerialValue(conn, "ingredient", "id"));
                }
                if (toSave.getPrice() != null) {
                    ps.setDouble(2, toSave.getPrice());
                } else {
                    ps.setNull(2, Types.DOUBLE);
                }
                ps.setString(3, toSave.getName());
                ps.setString(4, toSave.getCategory().name());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    ingredientId = rs.getInt(1);
                }
            }

            insertIngredientStockMovements(conn, toSave);

            conn.commit();
            return findIngredientById(ingredientId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertIngredientStockMovements(Connection conn, Ingredient ingredient) {
        List<StockMovement> stockMovementList = ingredient.getStockMovementList();
        String sql = """
                insert into stock_movement(id, id_ingredient, quantity, type, unit, creation_datetime)
                values (?, ?, ?, ?::movement_type, ?::unit, ?)
                on conflict (id) do nothing
                """;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            for (StockMovement stockMovement : stockMovementList) {
                if (ingredient.getId() != null) {
                    preparedStatement.setInt(1, ingredient.getId());
                } else {
                    preparedStatement.setInt(1, getNextSerialValue(conn, "StockMovement", "id"));
                }
                preparedStatement.setInt(2, ingredient.getId());
                preparedStatement.setDouble(3, stockMovement.getValue().getQuantity());
                preparedStatement.setObject(4, stockMovement.getType());
                preparedStatement.setObject(5, stockMovement.getValue().getUnit());
                preparedStatement.setTimestamp(6, Timestamp.from(stockMovement.getCreationDatetime()));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Order findOrderByReference(String reference) {
        DBConnection dbConnection = new DBConnection();
        try (Connection connection = dbConnection.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement("""
                    select id, reference, creation_datetime from "order" where reference like ?""");
            preparedStatement.setString(1, reference);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                Order order = new Order();
                Integer idOrder = resultSet.getInt("id");
                order.setId(idOrder);
                order.setReference(resultSet.getString("reference"));
                order.setCreationDatetime(resultSet.getTimestamp("creation_datetime").toInstant());
                order.setDishOrderList(findDishOrderByIdOrder(idOrder));
                return order;
            }
            throw new RuntimeException("Order not found with reference " + reference);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<DishOrder> findDishOrderByIdOrder(Integer idOrder) {
        DBConnection dbConnection = new DBConnection();
        Connection connection = dbConnection.getConnection();
        List<DishOrder> dishOrders = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    """
                            select id, id_dish, quantity from dish_order where dish_order.id_order = ?
                            """);
            preparedStatement.setInt(1, idOrder);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Dish dish = findDishById(resultSet.getInt("id_dish"));
                DishOrder dishOrder = new DishOrder();
                dishOrder.setId(resultSet.getInt("id"));
                dishOrder.setQuantity(resultSet.getInt("quantity"));
                dishOrder.setDish(dish);
                dishOrders.add(dishOrder);
            }
            dbConnection.closeConnection(connection);
            return dishOrders;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkStockAvailability(Order order) {
        Instant now = Instant.now();

        for (DishOrder dishOrder : order.getDishOrderList()) {
            Dish dish = dishOrder.getDish();
            int dishQuantity = dishOrder.getQuantity();

            for (DishIngredient dishIngredient : dish.getDishIngredients()) {
                Ingredient ingredient = dishIngredient.getIngredient();

                double requiredQuantity = dishIngredient.getQuantityRequired() * dishQuantity;

                double availableQuantity = ingredient.getStockValueAt(now).getQuantity();

                if (availableQuantity < requiredQuantity) {
                    throw new RuntimeException(
                            "Insufficient stock for ingredient: " + ingredient.getName());
                }
            }
        }
    }

    public Order saveOrder(Order orderToSave) {
        DBConnection dbConnection = new DBConnection();

        checkStockAvailability(orderToSave);

        String insertOrderSql = """
                    INSERT INTO "order" (id, reference, creation_datetime)
                    VALUES (?, ?, ?)
                    RETURNING id;
                """;

        String insertDishOrderSql = """
                    INSERT INTO dish_order (id, id_order, id_dish, quantity)
                    VALUES (?, ?, ?, ?);
                """;

        String insertStockMovementSql = """
                    INSERT INTO stockMovement
                    (id, id_ingredient, quantity, type, unit, creation_datetime)
                    VALUES (?, ?, ?, ?::movement_type, ?::unit, ?)
                    ON CONFLICT (id) DO NOTHING;
                """;

        try (Connection conn = dbConnection.getConnection()) {
            conn.setAutoCommit(false);

            Integer orderId;
            try (PreparedStatement ps = conn.prepareStatement(insertOrderSql)) {
                ps.setInt(1, getNextSerialValue(conn, "order", "id"));
                ps.setString(2, orderToSave.getReference());
                ps.setTimestamp(3, Timestamp.from(orderToSave.getCreationDatetime()));

                ResultSet rs = ps.executeQuery();
                rs.next();
                orderId = rs.getInt(1);
            }

            try (PreparedStatement ps = conn.prepareStatement(insertDishOrderSql)) {
                for (DishOrder dishOrder : orderToSave.getDishOrderList()) {
                    ps.setInt(1, getNextSerialValue(conn, "dish_order", "id"));
                    ps.setInt(2, orderId);
                    ps.setInt(3, dishOrder.getDish().getId());
                    ps.setInt(4, dishOrder.getQuantity());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            try (PreparedStatement ps = conn.prepareStatement(insertStockMovementSql)) {
                Instant now = Instant.now();

                for (DishOrder dishOrder : orderToSave.getDishOrderList()) {
                    Dish dish = dishOrder.getDish();
                    int dishQuantity = dishOrder.getQuantity();

                    for (DishIngredient dishIngredient : dish.getDishIngredients()) {
                        Ingredient ingredient = dishIngredient.getIngredient();

                        double usedQuantity = dishIngredient.getQuantityRequired() * dishQuantity;

                        ps.setInt(1, getNextSerialValue(conn, "stockMovement", "id"));
                        ps.setInt(2, ingredient.getId());
                        ps.setDouble(3, usedQuantity);
                        ps.setString(4, MovementTypeEnum.OUT.name());
                        ps.setString(5, ingredient.getStockValueAt(now).getUnit().name());
                        ps.setTimestamp(6, Timestamp.from(now));
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }

            conn.commit();

            return findOrderByReference(orderToSave.getReference());

        } catch (SQLException e) {
            throw new RuntimeException("Error while saving order", e);
        }
    }
}