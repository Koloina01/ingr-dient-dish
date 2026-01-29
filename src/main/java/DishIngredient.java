public class DishIngredient {
    private Integer id;
    private Dish dish;
    private Ingredient ingredient;
    private Double quantityRequired;
    private UnitType unit;

    public DishIngredient() {
    }
    
    public DishIngredient(Integer id, Dish dish, Ingredient ingredient, Double quantityRequired, UnitType unit) {
        this.id = id;
        this.dish = dish;
        this.ingredient = ingredient;
        this.quantityRequired = quantityRequired;
        this.unit = unit;
    }


    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }
    public Dish getDish() { 
        return dish;
    }
    public Ingredient getIngredient() {
        return ingredient;
    }
     public Double getQuantityRequired() {
        return quantityRequired;
    }
    public UnitType getUnit() {
        return unit;
    }
    public void setDish(Dish dish) {
        this.dish = dish;
    }
    public void setIngredient(Ingredient ingredient) {
        this.ingredient = ingredient;
    }
    public void setQuantityRequired(Double quantityRequired) {
        this.quantityRequired = quantityRequired;
    }
    public void setUnit(UnitType unit) {
        this.unit = unit;
    }

    @Override
    public String toString() {
        return "DishIngredient{" +
                "id=" + id +
                ", dish=" + (dish != null ? dish.getName() : null) +
                ", ingredient=" + (ingredient != null ? ingredient.getName() : null) +
                ", quantityRequired=" + quantityRequired +
                ", unit=" + unit +
                '}';
    }
}
