package ru.digitalhabbits.homework2;

import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private final Exchanger<List<String>> exchanger;
    private final String resultFileName;

    public FileWriter(Exchanger<List<String>> exchanger, String resultFileName) {
        this.exchanger = exchanger;
        this.resultFileName = resultFileName;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        try (BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(resultFileName))) {
            while (!currentThread().isInterrupted()) {
                List<String> data = exchanger.exchange(null);
                for (String line : data) {
                    writer.write(line + "\n");
                }
                writer.flush();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
