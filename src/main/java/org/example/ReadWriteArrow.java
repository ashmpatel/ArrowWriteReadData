package org.example;


import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;

import static java.util.Arrays.asList;

public class ReadWriteArrow {

    public static void main(String[] args) {

        System.out.println("Started");

        writeArrowFile();
        readArrowFile();
        projectionArrowRead();
        queryArrowData();

        System.out.println("Completed");
    }


    /**
     * Create an arrow file and write the data to it
     */
    public static void writeArrowFile() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try (
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
            ) {
                VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
                nameVector.allocateNew(3);
                nameVector.set(0, "David".getBytes());
                nameVector.set(1, "Gladis".getBytes());
                nameVector.set(2, "Juan".getBytes());

                IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
                ageVector.allocateNew(3);
                ageVector.set(0, 10);
                ageVector.set(1, 20);
                ageVector.set(2, 30);
                vectorSchemaRoot.setRowCount(3);

                File file = new File("random_access_to_file.arrow");
                try (
                        FileOutputStream fileOutputStream = new FileOutputStream(file);
                        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
                ) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                    System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Read the data in the arrow file and display it
     */
    public static void readArrowFile() {
        File file = new File("random_access_to_file.arrow");

        try (
                BufferAllocator rootAllocator = new RootAllocator();
                FileInputStream fileInputStream = new FileInputStream(file);
                ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ) {
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Some metadata about the stuff we stored
     */
    public static void queryArrowData() {
        String uri = "file:///c:/code/arrowreadwrite/random_access_to_file.arrow";
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.ARROW_IPC, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            int count = 1;
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    System.out.println("Number of rows per batch[" + count++ + "]: " + root.getRowCount());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Read only the columns we want
     */
    public static void projectionArrowRead() {
        String uri = "file:///c:/code/arrowreadwrite/random_access_to_file.arrow";
        String[] projection = new String[]{"name"};
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768, Optional.of(projection));
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.ARROW_IPC, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    System.out.print(root.contentToTSVString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


