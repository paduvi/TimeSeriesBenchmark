package com.techpago;

import com.techpago.dao.IUserNotifyDao;
import org.apache.commons.cli.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class App implements CommandLineRunner {

    @Autowired
    @Qualifier("HBaseUserNotifyDao")
    private IUserNotifyDao hBaseUserNotifyDao;

    public static void main(String[] args) {
        WebApplicationType webType = WebApplicationType.NONE;

        new SpringApplicationBuilder(App.class).web(webType)
                .registerShutdownHook(true).run(args);
    }

    private static int getMode(String[] args) {
        Options options = new Options();

        Option modelOpt = new Option("m", "mode", true, "mode option");
        modelOpt.setRequired(true);
        options.addOption(modelOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLine cmd = parser.parse(options, args);

            return Integer.parseInt(cmd.getOptionValue("mode"));
        } catch (ParseException e) {
            formatter.printHelp("utility-name", options);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        int mode = getMode(args);
        switch (mode) {
            case 1: // benchmark hbase
                // TODO:
                break;
        }
    }
}
