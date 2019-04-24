package ragna.pipeline.director.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ReportResults {
    private String fileName;
    private Long uniqueClients;
    private Long uniqueSalesmen;
    private String worstSalesman;
    private Long mostExpensiveSale;
}
