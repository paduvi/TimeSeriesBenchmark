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

    @Autowired
    @Qualifier("TimescaledbUserNotifyDao")
    private IUserNotifyDao timescaledbUserNotifyDao;

    @Autowired
    @Qualifier("KairosdbUserNotifyDao")
    private IUserNotifyDao kairosdbUserNotifyDao;

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

        Option numWriteEpochOpt = new Option("we", "write-epoch", true, "write epoch option");
        numWriteEpochOpt.setRequired(false);
        options.addOption(numWriteEpochOpt);

        Option numWriteThreadOpt = new Option("wt", "write-thread", true, "write thread option");
        numWriteThreadOpt.setRequired(false);
        options.addOption(numWriteThreadOpt);

        Option numFetchEpochOpt = new Option("fe", "fetch-epoch", true, "fetch epoch option");
        numFetchEpochOpt.setRequired(false);
        options.addOption(numFetchEpochOpt);

        Option numFetchThreadOpt = new Option("ft", "fetch-thread", true, "fetch thread option");
        numFetchThreadOpt.setRequired(false);
        options.addOption(numFetchThreadOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            int mode = Integer.parseInt(cmd.getOptionValue(modeOpt.getLongOpt()));
            int numWriteEpoch = Integer.parseInt(cmd.getOptionValue(numWriteEpochOpt.getLongOpt(), "10"));
            int numWriteThread = Integer.parseInt(cmd.getOptionValue(numWriteThreadOpt.getLongOpt(), "1"));
            int numFetchEpoch = Integer.parseInt(cmd.getOptionValue(numFetchEpochOpt.getLongOpt(), "10"));
            int numFetchThread = Integer.parseInt(cmd.getOptionValue(numFetchThreadOpt.getLongOpt(), "1"));

            BenchmarkService benchmarkService;
            switch (mode) {
                case 1: // benchmark hbase
                    benchmarkService = new BenchmarkService(
                            hBaseUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread
                    );
                    break;
                case 2: // benchmark timescaledb
                    benchmarkService = new BenchmarkService(
                            timescaledbUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread
                    );
                    break;
                case 3: // benmarl kairosdb
                    benchmarkService = new BenchmarkService(
                            kairosdbUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread
                            );
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + mode);
            }

            benchmarkService.benchmarkWrite();
            benchmarkService.benchmarkWriteCallback();

            benchmarkService.benchmarkFetchAsc();
            benchmarkService.benchmarkFetchDesc();

            benchmarkService.benchmarkFetchAscRampUp(10);
            benchmarkService.benchmarkFetchDescRampUp(10);

            System.exit(0);
        } catch (ParseException e) {
            formatter.printHelp("utility-name", options);
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
    }
}
