package revisionk2;

public class Dish {
    private int id;
    private String name;
    private DishTypeEnum dishType;

    public Dish(int id, String name, DishTypeEnum dishType) {
        this.id = id;
        this.name = name;
        this.dishType = dishType;
    }
    public int getId() {
        return id;
    }
    public String getName() {
        return name;
    }
    public DishTypeEnum getDishType() {
        return dishType;
    }

    public Double getDishPrice() {
        return null;
        // Placeholder for actual price calculation logic
    }
}