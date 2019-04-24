package ragna.pipeline.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaleItem {
    private Integer itemId;
    private BigDecimal itemQuantity;
    private BigDecimal itemPrice;
}
