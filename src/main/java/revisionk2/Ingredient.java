package revisionk2;

public class Ingredient {
    private int id;
    private String name;
    private Double price;
    private CategoryEnum category;
    private Dish dish;

    public Ingredient(int id, String name, Double price, CategoryEnum category, Dish dish) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.category = category;
        this.dish = dish;
    }

    public int getId() {
        return id;
    }
    public String getName() {
        return name;
    }
    public Double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Ingredient{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", category=" + category +
                ", dish=" + dish.getName() +
                '}';
    }
}
