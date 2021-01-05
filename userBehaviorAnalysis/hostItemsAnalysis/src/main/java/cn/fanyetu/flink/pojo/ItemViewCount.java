package cn.fanyetu.flink.pojo;

public class ItemViewCount {

    private Long itemId;

    private Integer count;

    private Long windowEnd;

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", count=" + count +
                ", windowEnd=" + windowEnd +
                '}';
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public ItemViewCount() {
    }

    public ItemViewCount(Long itemId, Integer count, Long windowEnd) {
        this.itemId = itemId;
        this.count = count;
        this.windowEnd = windowEnd;
    }
}
