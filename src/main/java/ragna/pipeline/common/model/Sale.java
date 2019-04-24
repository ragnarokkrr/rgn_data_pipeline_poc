package ragna.pipeline.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sale {
    private Long saleId;
    private String salesmanName;
    private List<SaleItem> saleItemList = new ArrayList<>();

    public static Sale ZERO_SALE = new Sale(0L, "ZERO", new ArrayList<>());

    public static Sale LAST_SALE = new Sale(0L, "LAST",
            Arrays.asList(new SaleItem(0, BigDecimal.ZERO, BigDecimal.ZERO)));

    public BigDecimal total() {
        return saleItemList.stream()
                .map(saleItem -> saleItem.getItemPrice().multiply(saleItem.getItemQuantity()))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
