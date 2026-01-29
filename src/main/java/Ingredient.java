import java.util.List;
import java.util.Objects;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;

public class Ingredient {
    private Integer id;
    private String name;
    private CategoryEnum category;
    private Double price;
    private List<StockMovement> stockMovementList;

    public Ingredient() {
    }

    public Ingredient(Integer id, String name, CategoryEnum category, Double price, List<StockMovement> stockMovementList) {
        this.id = id;
        this.name = name;
        this.category = category;
        this.price = price;
        this.stockMovementList = stockMovementList;
    }

    public Ingredient(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CategoryEnum getCategory() {
        return category;
    }

    public void setCategory(CategoryEnum category) {
        this.category = category;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public List<StockMovement> getStockMovementList() {
        return stockMovementList;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        Ingredient that = (Ingredient) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name) && category == that.category
                && Objects.equals(price, that.price) && Objects.equals(stockMovementList, that.stockMovementList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, category, price, stockMovementList);
    }

    @Override
    public String toString() {
        return "Ingredient{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", category=" + category +
                ", price=" + price +
                ", stockMovementList=" + stockMovementList +
                '}';
    }

    public void setStockMovementList(List<StockMovement> stockMovementList) {
        this.stockMovementList = stockMovementList;
    }

    public StockValue getStockValueAt(Instant instant) {
        double totalQuantity = 0.0;
        UnitType unit = UnitType.KG;

        if (stockMovementList == null) {
            return new StockValue(totalQuantity, unit);
        }

        for (StockMovement movement : stockMovementList) {
            if (movement.getCreationDatetime().isAfter(instant)) {
                continue;
            }

            if (movement.getType() == MovementTypeEnum.IN) {
                totalQuantity += movement.getValue().getQuantity();
            } else {
                totalQuantity -= movement.getValue().getQuantity();
            }
        }

        return new StockValue(totalQuantity, unit);/*
        manao if stockMovement null de mreturn null lo;
        Puis,manao anle List<stockMovements = new ArrayList<>(); ny option1
        ny option 2 manao stream filter de atao toList() aveo.
        de apres anzay vo manao calcul ndrai de ze miditra reetra izany oe de type 'in' atao + de  atao - izy reefa 'out' le type
        de apres vo returner new stockVaalue de iny new stockValue iny no atao setQuantity(movementIn - movementOut),de stockValue.setUnit(null); --methode simple daoly zany

        fa ny methode afa ko mapiasa anle Map sy flatMap sy ny sum sy ny filter sy ny flatMapToDouble.
         */
    }
}