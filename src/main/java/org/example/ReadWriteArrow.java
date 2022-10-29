package org.example;


import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static java.util.Arrays.asList;

public class ReadWriteArrow {

        public static void main(String[] args) {

            System.out.println("Started");

            writeArrowFile();
            readArrowFile();

            System.out.println("Completed");
        }


        public  static void writeArrowFile() {
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

}


