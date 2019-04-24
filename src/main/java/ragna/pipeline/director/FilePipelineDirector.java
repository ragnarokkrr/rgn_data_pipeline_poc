package ragna.pipeline.director;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Repository;
import ragna.pipeline.common.model.Client;
import ragna.pipeline.common.model.Sale;
import ragna.pipeline.common.model.Salesman;
import ragna.pipeline.director.model.FileDirector;
import ragna.pipeline.director.model.ReportResults;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Keep track of file processing across all components using Redis.
 *
 */
@Repository
@Slf4j
public class FilePipelineDirector {
    private static final String KEY = "FilePipeline";
    private static final String UNIQUE_CLIENTS = "uniqueClients";
    private static final String UNIQUE_SALESMEN = "UniquerSalesmen";
    private static final String MOST_EXPENSIVE_SALE = "mostExpensiveSale";
    private static final String SALESMEN_TOTALS = "SalesmenTotals";
    private static final String JOHN_DOE = "John Doe"; // Unknown / Not Found person
    private static final long INVALID_SALE = -1L;

    private RedisTemplate<String, Object> redisTemplate;
    private HashOperations hashOperations;
    private ValueOperations<String, Object> valueOperations;
    private SetOperations setOperations;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public FilePipelineDirector(RedisTemplate<String, Object> redisTemplate){
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    private void init(){
        hashOperations = redisTemplate.opsForHash();
        setOperations = redisTemplate.opsForSet();
        valueOperations = redisTemplate.opsForValue();
    }

    public void beginProcessing(String filename) {
        FileDirector fileDirector = new FileDirector(filename, FileDirector.FileStage.IN_PROCESS);
        try {
            hashOperations.put(KEY, fileDirector.getFileName(), fileDirector);
            log.info("Pipeline initialized: {}", fileDirector);
        } catch (Throwable e ){
            log.error("Error Initializing Pipeline: " + filename, e);
        }
    }

    public void finishProcessing(String filename) {
        FileDirector fileDirector = new FileDirector(filename, FileDirector.FileStage.PROCESSED);
        hashOperations.put(KEY, fileDirector.getFileName(), fileDirector);
        log.info("Pipeline finalized: {}", fileDirector);
    }

    public ReportResults downloadReport(String filename) {
        FileDirector fileDirector = new FileDirector(filename, FileDirector.FileStage.DOWNLOADED);
        hashOperations.put(KEY, fileDirector.getFileName(), fileDirector);
        log.info("Pipeline downloaded: {}", fileDirector);

        ReportResults reportResults = collectReportResults(filename);

        return reportResults;
    }

    public FileDirector get(String filename) {
        FileDirector fileDirector = (FileDirector) hashOperations.get(KEY, filename);
        return fileDirector;
    }

    public void addClient(String filename, Client client) {
        setOperations.add(keyName(filename, UNIQUE_CLIENTS), client.getCnpj());
        log.debug("Client added: {} {}", filename, client);
    }

    public void addSalesman(String filename, Salesman salesman) {
        setOperations.add(keyName(filename, UNIQUE_SALESMEN), salesman.getName());
        log.debug("Salesman added: {} {}", filename, salesman);
    }

    public void processSales(String filename, Sale sale) {
        final String salesmenTotals = keyName(filename, SALESMEN_TOTALS);

        updateSalesmenTotal(sale, salesmenTotals);

        final String mostExpensiveKey = keyName(filename, MOST_EXPENSIVE_SALE);

        updateMostExpensiveSale(sale, mostExpensiveKey);

        log.debug("Sale processed: {} {}", filename, sale);
    }

    private ReportResults collectReportResults(String filename) {
        Long uniqueClients = setOperations.size(keyName(filename, UNIQUE_CLIENTS));
        Long uniqueSalesmen = setOperations.size(keyName(filename, UNIQUE_SALESMEN));

        String worstSalesman = findWorstSalesman(filename);

        Long mostExpensiveSaleId = getMostExpensiveSale(filename);

        ReportResults reportResults = new ReportResults();
        reportResults.setFileName(filename);
        reportResults.setUniqueClients(uniqueClients);
        reportResults.setUniqueSalesmen(uniqueSalesmen);
        reportResults.setWorstSalesman(worstSalesman);
        reportResults.setMostExpensiveSale(mostExpensiveSaleId);
        return reportResults;
    }

    private Long getMostExpensiveSale(String filename) {
        final String mostExpensiveKey = keyName(filename, MOST_EXPENSIVE_SALE);

        String rawMostExpensiveSale = (String) valueOperations.get(mostExpensiveKey);

        try {
            Sale mostExpensiveSale = objectMapper.readValue(rawMostExpensiveSale, Sale.class);
            return mostExpensiveSale.getSaleId();
        } catch (IOException e) {
            log.error("Error retrieving most expensive sale:", e);
            return INVALID_SALE;
        }
    }

    private String findWorstSalesman(String filename) {
        final String salesmenTotals = keyName(filename, SALESMEN_TOTALS);
        Map<String, BigDecimal> salesMenTotals =
                (Map<String, BigDecimal>) hashOperations.entries(salesmenTotals);

        Map.Entry<String, BigDecimal> worstSalesmanEntry = salesMenTotals
                .entrySet().stream()
                .min(Comparator.comparing(Map.Entry::getValue))
                .orElse(new AbstractMap.SimpleImmutableEntry(
                        JOHN_DOE, BigDecimal.ZERO));

        return worstSalesmanEntry.getKey();
    }

    private void updateSalesmenTotal(Sale sale, String salesmenTotals) {
        redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                try {
                    operations.watch(salesmenTotals);

                    BigDecimal storedTotalSales = (BigDecimal) operations.opsForHash().get(salesmenTotals,
                            sale.getSalesmanName());

                    operations.multi();

                    BigDecimal summation = storedTotalSales == null ? BigDecimal.ZERO : storedTotalSales;

                    summation = summation.add(sale.total());

                    operations.opsForHash().put(salesmenTotals,
                            sale.getSalesmanName(), summation);

                } catch (Throwable e) {
                    log.error("SALESMEN summation error", e);
                }
                return operations.exec();
            }
        });
    }

    private void updateMostExpensiveSale(Sale sale, String mostExpensiveKey) {

        redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                try {
                    operations.watch(mostExpensiveKey);
                    // @TODO Fix TypeConverter Registering problem
                    String rawStoredSale = (String) operations.opsForValue().get(mostExpensiveKey);

                    operations.multi();
                    if (rawStoredSale == null) {
                        operations.opsForValue().set(mostExpensiveKey, objectMapper.writeValueAsString(sale));
                    } else  {
                        Sale storedSale = objectMapper.readValue(rawStoredSale, Sale.class);
                        if (storedSale.total().compareTo(sale.total()) < 0) {
                            operations.opsForValue().set(mostExpensiveKey, objectMapper.writeValueAsString(sale));
                        }
                    }
                } catch (Throwable e) {
                    log.error("EXPENSIVIEST SALE error", e);
                }
                return operations.exec();
            }
        });
    }

    private String keyName(String filename, String keyName) {
        return String.format("%s::%s::%s", KEY, filename, keyName);
    }
}
