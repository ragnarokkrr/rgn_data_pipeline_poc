package ragna.pipeline.consumer.stats;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

    @Autowired
    private FileStats fileStats;

    @RequestMapping("/file/stats")
    public FileStats getStats() {
        return fileStats;
    }
}
