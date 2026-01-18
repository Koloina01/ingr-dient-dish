public class DishIngredient {
    private Integer id;
    private Dish dish;
    private Ingredient ingredient;
    private Double quantityRequired;
    private UnitType unit;



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
}
