package ragna.pipeline.consumer.stats;

import lombok.Data;
import org.springframework.stereotype.Component;
import ragna.pipeline.common.model.Client;
import ragna.pipeline.common.model.Sale;
import ragna.pipeline.common.model.Salesman;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Local Memory stats used for local debugging.
 */
@Component
@Data
public class FileStats {
    private Set uniqueClients = new HashSet<String>();
    private Set uniqueSalesman = new HashSet<String>();
    private Map<String, BigDecimal> salesmanTotals = new HashMap<>();
    private Sale mostExpensiveSale = Sale.ZERO_SALE;

    public void processSales(Sale sale) {
        if(sale.total().compareTo(mostExpensiveSale.total()) > 0) {
            mostExpensiveSale = sale;
        }

        salesmanTotals.computeIfPresent(sale.getSalesmanName()
                , (k, v) -> v.add(sale.total()));
        salesmanTotals.computeIfAbsent(sale.getSalesmanName()
                , k -> sale.total());
    }

    public void processClient(Client client) {
        uniqueClients.add(client.getCnpj());
    }

    public void processSalesman(Salesman salesman) {
        uniqueSalesman.add(salesman.getName());
    }
}
