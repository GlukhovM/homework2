package ru.digitalhabbits.homework2;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);
        final File file = new File(processingFileName);
        ExecutorService lineProcessorService = Executors.newFixedThreadPool(CHUNK_SIZE);
        Exchanger<List<String>> exchanger = new Exchanger<>();

        Thread writerThread = new Thread(new FileWriter(exchanger, resultFileName));
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                List<String> linesToProcess = new ArrayList<>();
                while (linesToProcess.size() < CHUNK_SIZE && scanner.hasNext()) {
                    linesToProcess.add(scanner.nextLine());
                }
                final int tempLinesCount = linesToProcess.size();

                Phaser phaser = new Phaser(tempLinesCount + 1);
                String[] tempLines = new String[tempLinesCount];
                for (int i = 0; i < linesToProcess.size(); i++) {
                    int index = i;
                    lineProcessorService.submit(() -> {
                        tempLines[index] = new LineCounterProcessor().process(linesToProcess.get(index)).toString(("%s %d"));
                        phaser.arrive();
                    });
                }
                phaser.arriveAndAwaitAdvance();

                List<String> linesToWrite = new ArrayList<>(Arrays.asList(tempLines));
                exchanger.exchange(linesToWrite);
            }
        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }

        lineProcessorService.shutdown();
        writerThread.interrupt();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
