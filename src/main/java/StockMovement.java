import java.time.Instant;

public class StockMovement {
    private Integer id;
    private StockValue value;
    private MovementTypeEnum type;
    private Instant creationDatetime;

    public StockMovement(Integer id, StockValue value, MovementTypeEnum type, Instant creationDatetime) {
        this.id = id;
        this.value = value;
        this.type = type;
        this.creationDatetime = creationDatetime;
    }

    public Integer getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public StockValue getValue() {
        return value;
    }
    public void setValue(StockValue value) {
        this.value = value;
    }
    public MovementTypeEnum getType() {
        return type;
    }
    public void setType(MovementTypeEnum type) {
        this.type = type;
    }
    public Instant getCreationDatetime() {
        return creationDatetime;
    }
    public void setCreationDatetime(Instant creationDatetime) {
        this.creationDatetime = creationDatetime;
    }
}
