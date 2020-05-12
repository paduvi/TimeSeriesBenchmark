package io.dogy;

import io.dogy.dao.IUserNotifyDao;
import io.dogy.dao.impl.HBaseUserNotifyDao;
import io.dogy.dao.impl.InfluxDbUserNotifyDao;
import io.dogy.dao.impl.OpentsdbUserNotifyDao;
import io.dogy.dao.impl.TimescaleDbUserNotifyDao;
import io.dogy.service.BenchmarkService;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class App implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    @Autowired
    private ApplicationContext context;


//    @Autowired
//    @Qualifier("KairosdbUserNotifyDao")
//    private IUserNotifyDao kairosdbUserNotifyDao;

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
    public void run(String... args) {
        Options options = new Options();

        Option modeOpt = new Option("m", "mode", true, "mode option");
        modeOpt.setRequired(false);
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

        Option numBootstrapOpt = new Option("b", "bootstrap", true, "bootstrap option");
        numBootstrapOpt.setRequired(false);
        options.addOption(numBootstrapOpt);

        Option verboseOpt = new Option("v", "verbose", true, "verbose option");
        verboseOpt.setRequired(false);
        options.addOption(verboseOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            int mode = Integer.parseInt(cmd.getOptionValue(modeOpt.getLongOpt(), "-1"));
            int numWriteEpoch = Integer.parseInt(cmd.getOptionValue(numWriteEpochOpt.getLongOpt(), "10"));
            int numWriteThread = Integer.parseInt(cmd.getOptionValue(numWriteThreadOpt.getLongOpt(), "1"));
            int numFetchEpoch = Integer.parseInt(cmd.getOptionValue(numFetchEpochOpt.getLongOpt(), "10"));
            int numFetchThread = Integer.parseInt(cmd.getOptionValue(numFetchThreadOpt.getLongOpt(), "1"));
            int numBootstrap = Integer.parseInt(cmd.getOptionValue(numBootstrapOpt.getLongOpt(), "0"));
            boolean verbose = Boolean.parseBoolean(cmd.getOptionValue(verboseOpt.getLongOpt(), "false"));

            AutowireCapableBeanFactory factory = context.getAutowireCapableBeanFactory();

            BenchmarkService benchmarkService;
            switch (mode) {
                case 1: // benchmark hbase
                    IUserNotifyDao hBaseUserNotifyDao = factory.createBean(HBaseUserNotifyDao.class);
                    benchmarkService = new BenchmarkService(
                            hBaseUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread,
                            numBootstrap
                    );
                    break;
                case 2: // benchmark timescaledb
                    IUserNotifyDao timescaledbUserNotifyDao = factory.createBean(TimescaleDbUserNotifyDao.class);
                    benchmarkService = new BenchmarkService(
                            timescaledbUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread,
                            numBootstrap
                    );
                    break;

                case 3: // benchmark timescaledb
                    IUserNotifyDao influxDbUserNotifyDao = factory.createBean(InfluxDbUserNotifyDao.class);
                    benchmarkService = new BenchmarkService(
                            influxDbUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread,
                            numBootstrap
                    );
                    break;
                case 4: //benchmark opentsdb
                    IUserNotifyDao opentsdbUserNotifyDao = factory.createBean(OpentsdbUserNotifyDao.class);
                    benchmarkService = new BenchmarkService(
                            opentsdbUserNotifyDao,
                            numWriteEpoch,
                            numWriteThread,
                            numFetchEpoch,
                            numFetchThread,
                            numBootstrap
                    );
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + mode);
            }

            benchmarkService.setVerbose(verbose);
            benchmarkService.bootstrap();
            long minTime = System.currentTimeMillis();

            benchmarkService.benchmarkWrite();
            long maxTime = System.currentTimeMillis();

            if (numWriteEpoch * 100 > numBootstrap) {
                benchmarkService.bootstrap();
            }
            benchmarkService.benchmarkWriteCallback();

            if (numBootstrap != 0 && numWriteEpoch * 100 > numBootstrap) {
                minTime = System.currentTimeMillis();
                benchmarkService.bootstrap();
                maxTime = System.currentTimeMillis();
            }

            benchmarkService.benchmarkFetchAsc(minTime, maxTime);
            benchmarkService.benchmarkFetchDesc(minTime, maxTime);

            benchmarkService.benchmarkFetchAscRampUp(10, minTime, maxTime);
            benchmarkService.benchmarkFetchDescRampUp(10, minTime, maxTime);

            System.exit(0);
        } catch (ParseException e) {
            formatter.printHelp("utility-name", options);
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
    }
}
