/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.xml;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import fi.iki.elonen.NanoHTTPD;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.Assert.fail;

public final class JavaXmlSuite {

    private static final int numBooks = 12;
    private static final String booksFile = "src/test/resources/books.xml";
    private static final String booksFileTag = "book";

    private SparkSession spark;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        spark = SparkSession.builder().
            master("local[2]").
            appName("XmlSuite").
            config("spark.ui.enabled", false).
            getOrCreate();
        tempDir = Files.createTempDirectory("JavaXmlSuite");
        tempDir.toFile().deleteOnExit();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    private Path getEmptyTempDir() throws IOException {
        return Files.createTempDirectory(tempDir, "test");
    }

    @Test
    public void testXmlParser() {
        Dataset<Row> df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(spark, booksFile);
        String prefix = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX();
        long result = df.select(prefix + "id").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testLoad() {
        Map<String, String> options = new HashMap<>();
        options.put("rowTag", booksFileTag);
        Dataset<Row> df = spark.read().options(options).format("xml").load(booksFile);
        long result = df.select("description").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testSave() throws IOException {
        Path booksPath = getEmptyTempDir().resolve("booksFile");

        Dataset<Row> df = (new XmlReader()).withRowTag(booksFileTag).xmlFile(spark, booksFile);
        df.select("price", "description").write().format("xml").save(booksPath.toString());

        Dataset<Row> newDf = (new XmlReader()).xmlFile(spark, booksPath.toString());
        long result = newDf.select("price").count();
        Assert.assertEquals(result, numBooks);
    }

    @Test
    public void testEntityExpansion() {
        ExploitServer.withServer(s -> fail("Should not have made request"), () -> {
            String testFile = "src/test/resources/books-external-entity.xml";
            Map<String, String> options = new HashMap<>();
            options.put("rowTag", booksFileTag);
            Dataset<Row> df = spark.read().options(options).format("xml").load(testFile);
            long result = df.select("description").count();
            Assert.assertEquals(result, numBooks);
        });
    }

    private static class ExploitServer extends NanoHTTPD implements AutoCloseable {
        private final Consumer<IHTTPSession> onRequest;

        public ExploitServer(Consumer<IHTTPSession> onRequest) throws IOException {
            super(61932);
            this.onRequest = onRequest;
        }

        @Override
        public Response serve(IHTTPSession session) {
            onRequest.accept(session);
            return newFixedLengthResponse("<!ENTITY % data SYSTEM \"file://pom.xml\">\n");
        }

        public static void withServer(Consumer<IHTTPSession> onRequest, Runnable func) {
            try(ExploitServer server = new ExploitServer(onRequest)) {
                server.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
                func.run();
            } catch(IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {
            this.stop();
        }
    }
}
