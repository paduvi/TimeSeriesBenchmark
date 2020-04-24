package com.techpago;

import com.techpago.dao.IUserNotifyDao;
import com.techpago.service.BenchmarkService;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class App implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    @Autowired
    @Qualifier("HBaseUserNotifyDao")
    private IUserNotifyDao hBaseUserNotifyDao;

    public static void main(String[] args) {
        WebApplicationType webType = WebApplicationType.NONE;

        try {
            new SpringApplicationBuilder(App.class).web(webType)
                    .registerShutdownHook(true).run(args);
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        Options options = new Options();

        Option modeOpt = new Option("m", "mode", true, "mode option");
        modeOpt.setRequired(true);
        options.addOption(modeOpt);

        Option numEpochOpt = new Option("e", "epoch", true, "epoch option");
        numEpochOpt.setRequired(false);
        options.addOption(numEpochOpt);

        Option numThreadOpt = new Option("t", "thread", true, "thread option");
        numThreadOpt.setRequired(false);
        options.addOption(numThreadOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            int mode = Integer.parseInt(cmd.getOptionValue("mode"));
            int numEpoch = Integer.parseInt(cmd.getOptionValue("epoch", "10"));
            int numThread = Integer.parseInt(cmd.getOptionValue("thread", "1"));

            logger.info(String.format("mode: %s - numEpoch: %s - numThread: %s", mode, numEpoch, numThread));

            switch (mode) {
                case 1: // benchmark hbase
                    BenchmarkService hbaseBenchmark = new BenchmarkService(hBaseUserNotifyDao, numEpoch, numThread);
                    hbaseBenchmark.start();
                    break;
            }
        } catch (ParseException e) {
            formatter.printHelp("utility-name", options);
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
    }
}
